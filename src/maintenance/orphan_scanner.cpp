#include "maintenance/orphan_scanner.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <typeinfo>

#ifdef _WIN32
#include "duckdb/common/path.hpp"
#include "duckdb/common/windows.hpp"
#else
#include <cerrno>
#include <sys/stat.h>
#endif

namespace duckdb {

namespace {

bool HasUriScheme(const string &path) {
	auto colon = path.find("://");
	if (colon == string::npos || colon == 0 || !StringUtil::CharacterIsAlpha(path[0])) {
		return false;
	}
	for (idx_t i = 1; i < colon; ++i) {
		auto c = path[i];
		if (!StringUtil::CharacterIsAlpha(c) && !StringUtil::CharacterIsDigit(c) && c != '+' && c != '-' && c != '.') {
			return false;
		}
	}
	return true;
}

const char *CanonicalS3Scheme(const string &scheme, const unordered_set<string> &aliases) {
	if (scheme == "s3" || scheme == "s3a" || scheme == "s3n" || aliases.count(scheme)) {
		return "s3";
	}
	if (scheme == "gcs" || scheme == "gs") {
		return "gs";
	}
	return scheme == "r2" ? "r2" : nullptr;
}

idx_t PathStart(const string &path) {
	if (IsLocalOrphanPath(path)) {
		return 0;
	}
	return MinValue(path.find('/', path.find("://") + 3), path.size());
}

bool HasAmbiguousPathComponents(const string &path, bool require_file_key) {
	const auto path_start = PathStart(path);
	// Hadoop Path collapses components that other clients may interpret literally.
	return path.find("//", path_start) != string::npos || path.find("/./", path_start) != string::npos ||
	       (path.size() - path_start >= 2 && StringUtil::EndsWith(path, "/.")) ||
	       (require_file_key && (path_start == path.size() || path.back() == '/'));
}

void ValidateS3PathComponents(const string &path, bool require_file_key = false) {
	if (HasAmbiguousPathComponents(path, require_file_key)) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files cannot verify S3 object keys with empty or '.' path components: %s", path);
	}
}

bool HasS3UriSuffix(const string &path) {
	return path.find_first_of("?#") != string::npos;
}

void ValidateS3UriSuffix(const string &path) {
	// Iceberg FileIO implementations can treat these suffixes as URI syntax.
	if (HasS3UriSuffix(path)) {
		throw InvalidConfigurationException("iceberg_remove_orphan_files cannot verify S3 paths containing '?' or '#'");
	}
}

string DirectoryPrefix(const string &path) {
	return StringUtil::EndsWith(path, "/") ? path : path + "/";
}

string S3Bucket(const string &path) {
	const auto start = path.find("://") + 3;
	return path.substr(start, PathStart(path) - start);
}

const char *S3_SECRET_TYPES[] = {"s3", "r2", "gcs", "aws"};

unique_ptr<SecretEntry> LookupStorageSecret(ClientContext &context, CatalogTransaction transaction,
                                            const string &path) {
	auto &manager = SecretManager::Get(context);
	// Match the type precedence of HTTPFS's S3 resolver.
	for (const auto *type : S3_SECRET_TYPES) {
		auto match = manager.LookupSecret(transaction, path, type);
		if (match.HasMatch()) {
			return std::move(match.secret_entry);
		}
	}
	// HTTPFS also uses an HTTP secret for request headers/proxy when no S3 credential matched.
	Value merge(true);
	context.TryGetCurrentSetting("merge_http_secret_into_s3_request", merge);
	if (merge.IsNull() || merge.GetValue<bool>()) {
		return manager.LookupSecret(transaction, path, "http").secret_entry;
	}
	return nullptr;
}

constexpr const char *S3_GLOBAL_CONFIGURATION = "enable_global_s3_configuration";

bool IsS3Secret(const Identifier &type) {
	return std::any_of(std::begin(S3_SECRET_TYPES), std::end(S3_SECRET_TYPES),
	                   [&](const char *s3_type) { return type == s3_type; });
}

bool IsObjectStoreSecret(const Identifier &type) {
	return IsS3Secret(type) || type == "http";
}

bool SameStorageSecretRoute(optional_ptr<const SecretEntry> left, optional_ptr<const SecretEntry> right) {
	if (!left || !right) {
		return !left && !right;
	}
	if (left->secret->GetType() != right->secret->GetType() ||
	    left->secret->GetProvider() != right->secret->GetProvider()) {
		return false;
	}
	const auto &a = left->secret->Cast<KeyValueSecret>();
	const auto &b = right->secret->Cast<KeyValueSecret>();
	if (left->storage_mode != right->storage_mode || a.GetName() != b.GetName()) {
		if (a.GetProvider() != "config" || a.secret_map.count("refresh_info") || b.secret_map.count("refresh_info")) {
			return false;
		}
	}
	return std::equal(
	    a.secret_map.begin(), a.secret_map.end(), b.secret_map.begin(), b.secret_map.end(),
	    [](const auto &x, const auto &y) { return x.first == y.first && Value::NotDistinctFrom(x.second, y.second); });
}

bool IsSymbolicLink(const string &path) {
#ifdef _WIN32
	vector<wchar_t> wide_path(path.size() + 1);
	if (!MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.c_str(), -1, wide_path.data(),
	                         static_cast<int>(wide_path.size()))) {
		throw InvalidConfigurationException("iceberg_remove_orphan_files: invalid UTF-8 path: %s", path);
	}
	auto attributes = GetFileAttributesW(wide_path.data());
	if (attributes == INVALID_FILE_ATTRIBUTES) {
		auto error = GetLastError();
		if (error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND) {
			throw IOException("iceberg_remove_orphan_files: could not inspect path %s: Windows error %lu", path, error);
		}
		return false;
	}
	return (attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0;
#else
	struct stat status;
	if (lstat(path.c_str(), &status) != 0) {
		auto error = errno;
		if (error != ENOENT && error != ENOTDIR) {
			throw IOException({{"errno", std::to_string(error)}},
			                  "iceberg_remove_orphan_files: could not inspect path %s: %s", path, std::strerror(error));
		}
		return false;
	}
	return S_ISLNK(status.st_mode);
#endif
}

string ResolveListedPath(FileSystem &fs, const string &directory, const string &listed_path) {
	if (fs.IsPathAbsolute(listed_path)) {
		return fs.ExpandPath(listed_path);
	}
	return JoinOrphanPath(fs, directory, listed_path);
}

string NormalizeLocalOrphanPath(const string &path) {
#ifdef _WIN32
	auto key = StringUtil::Replace(path, "\\", "/");
#else
	const auto &key = path;
#endif
	bool absolute = !key.empty() && key[0] == '/';
	vector<string> parts;
	for (const auto &part : StringUtil::Split(key, '/')) {
		if (!part.empty() && part != ".") {
			parts.push_back(part);
		}
	}
	return (absolute ? "/" : "") + StringUtil::Join(parts, "/");
}

string NormalizeOrphanPath(const string &path) {
	return IsLocalOrphanPath(path) ? NormalizeLocalOrphanPath(path) : path;
}

bool HasNormalizedPathPrefix(const string &path, const string &prefix) {
	if (path == prefix) {
		return true;
	}
	return !prefix.empty() && StringUtil::StartsWith(path, prefix) &&
	       (prefix.back() == '/' || path[prefix.size()] == '/');
}

class OrphanFileSystem final : public OpenerFileSystem {
public:
	explicit OrphanFileSystem(OpenerFileSystem &fs) : fs(fs) {
	}
	explicit OrphanFileSystem(unique_ptr<OpenerFileSystem> private_fs)
	    : owned_fs(std::move(private_fs)), fs(*owned_fs) {
	}

	FileSystem &GetFileSystem() const override {
		return fs.GetFileSystem();
	}

	optional_ptr<FileOpener> GetOpener() const override {
		return fs.GetOpener();
	}

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override {
		// Avro requests caching even though the shared cache keys files only by path.
		flags.SetCachingMode(CachingMode::NO_CACHING);
		auto context =
		    owned_fs && !IsLocalOrphanPath(file.path) ? FileOpener::TryGetClientContext(GetOpener()) : nullptr;
		if (context) {
			// A listing can hold on to the prior HTTPFS state.
			context->registered_state->Remove("http_state");
		}
		auto handle = fs.OpenFile(file, flags, opener);
		if (context) {
			// The open handle owns its HTTP download state. Release the private connection's reference.
			context->registered_state->Remove("http_state");
		}
		return handle;
	}

private:
	unique_ptr<OpenerFileSystem> owned_fs;
	OpenerFileSystem &fs;
};

} // namespace

bool IsLocalOrphanPath(const string &path) {
#ifdef _WIN32
	if (path.size() >= 2 && StringUtil::CharacterIsAlpha(path[0]) && path[1] == ':') {
		return true;
	}
#endif
	return !HasUriScheme(path);
}

Connection OpenOrphanStorage(ClientContext &caller, const IcebergTable &table, const string &root,
                             const string &metadata_location, bool for_deletion) {
	if (!IsLocalOrphanPath(root) && !OrphanPathNormalizer(caller).GetS3Scheme(root)) {
		throw InvalidInputException("iceberg_remove_orphan_files: unsupported remote scan location %s", root);
	}
	// The caller owns the catalog transaction and its credential callback. File I/O has a private
	// settings map and transaction, and follows cancellation of the calling query.
	auto connection = Connection::CreateInternal(caller);
	auto &io = *connection.context;
	io.config.user_settings = caller.config.user_settings;
	io.logger = caller.logger;
	auto &original_fs = FileSystem::GetFileSystem(caller);
	auto &io_fs = ClientData::Get(io).client_file_system;
	auto original = dynamic_cast<OpenerFileSystem *>(&original_fs);
	if (original && IsLocalOrphanPath(root) && IsLocalOrphanPath(metadata_location)) {
		// Only the Avro reader needs a separate context to override its cache mode.
		// The underlying local filesystem uses the caller's opener and transaction.
		table.LoadCredentials(caller);
		io_fs = make_uniq<OrphanFileSystem>(*original);
		io.transaction.BeginTransaction();
		return connection;
	}
	auto isolated = dynamic_cast<OpenerFileSystem *>(io_fs.get());
	if (!original || !isolated || typeid(original_fs) != typeid(*io_fs) ||
	    &original->GetFileSystem() != &isolated->GetFileSystem() || !original->GetOpener() || !isolated->GetOpener() ||
	    typeid(*original->GetOpener()) != typeid(*isolated->GetOpener())) {
		throw InvalidConfigurationException("iceberg_remove_orphan_files cannot inherit the caller's file system");
	}
	io_fs = make_uniq<OrphanFileSystem>(unique_ptr_cast<FileSystem, OpenerFileSystem>(std::move(io_fs)));
	io.transaction.BeginTransaction();

	auto &manager = SecretManager::Get(caller);
	auto source = CatalogTransaction::GetSystemCatalogTransaction(caller);
	auto destination = CatalogTransaction::GetSystemCatalogTransaction(io);
	for (auto &entry : manager.AllSecrets(source)) {
		const bool transaction_secret = entry.persist_type == SecretPersistType::TRANSACTION;
		if (transaction_secret || entry.storage_mode == SecretManager::CONNECTION_STORAGE_NAME) {
			manager.RegisterSecret(destination, std::move(entry.secret), OnCreateConflict::ERROR_ON_CONFLICT,
			                       entry.persist_type, transaction_secret ? string() : entry.storage_mode);
		}
	}

	for (const auto &path : {root, metadata_location}) {
		if (!IsLocalOrphanPath(path)) {
			ExtensionHelper::AutoLoadExtensionForPath(*caller.db, path);
		}
	}
	table.LoadCredentials(caller, &io);

	auto &config = DBConfig::GetConfig(caller);
	auto set = [&](const char *name, Value value) {
		optional_ptr<const ConfigurationOption> option;
		auto index = config.TryGetSettingIndex(name, option);
		if (index.IsValid()) {
			io.config.user_settings.SetUserSetting(index.GetIndex(), std::move(value));
		}
	};
	Value use_global(true);
	caller.TryGetCurrentSetting(S3_GLOBAL_CONFIGURATION, use_global);
	const auto allow_global = use_global.GetValue<bool>();
	// Freeze registered extension settings for this S3 operation so an endpoint change cannot
	// send LIST and DELETE to different stores. Keep disabled global S3 fallbacks at global scope.
	for (const auto &entry : config.GetExtensionSettings()) {
		const auto &name = entry.first.GetIdentifierName();
		Value value;
		auto scope = caller.TryGetCurrentSetting(entry.first, value);
		if (!allow_global && (StringUtil::StartsWith(name, "s3_") || StringUtil::StartsWith(name, "gcs_")) && scope &&
		    scope.GetScope() == SettingScope::GLOBAL) {
			continue;
		}
		io.config.user_settings.SetUserSetting(entry.second.setting_index.GetIndex(), std::move(value));
	}
	// These are DuckDB core options, outside the extension setting registry.
	for (const auto *name : {"http_proxy", "http_proxy_username", "http_proxy_password"}) {
		Value value;
		caller.TryGetCurrentSetting(name, value);
		set(name, std::move(value));
	}
	set("home_directory", Value(original_fs.GetHomeDirectory()));
	set("httpfs_enable_credential_refresh", Value(false));
	set("enable_http_metadata_cache", Value(false));
	if (for_deletion && !IsLocalOrphanPath(root)) {
		set("http_retries", Value::UBIGINT(0));
	}
	return connection;
}

OrphanPathNormalizer::OrphanPathNormalizer(ClientContext &context_p, const string &scan_root,
                                           optional_ptr<ClientContext> caller)
    : context(context_p) {
	Value aliases;
	if (context.TryGetCurrentSetting("s3_url_scheme_aliases", aliases) && !aliases.IsNull() &&
	    aliases.type().id() == LogicalTypeId::LIST) {
		for (const auto &alias : ListValue::GetChildren(aliases)) {
			if (!alias.IsNull()) {
				s3_aliases.insert(StringUtil::Lower(StringValue::Get(alias)));
			}
		}
	}
	s3_scan_scheme = GetS3Scheme(scan_root);
	if (s3_scan_scheme) {
		auto &catalog = Catalog::GetSystemCatalog(context);
		if (caller && catalog.GetCatalogVersion(*caller) != catalog.GetCatalogVersion(context)) {
			caller_catalog = CatalogTransaction::GetSystemCatalogTransaction(*caller);
			// Shared secret storage reads the catalog snapshot. Connection and transaction secrets
			// come from the context: use the same cloned and vended credentials for both lookups.
			caller_catalog->context = &context;
		}
		s3_scan_bucket = S3Bucket(scan_root);
		ValidateS3Routes(scan_root);
	}
}

OrphanPathNormalizer::~OrphanPathNormalizer() = default;

unique_ptr<SecretEntry> OrphanPathNormalizer::ResolveStorageSecret(const string &path) const {
	auto selected = LookupStorageSecret(context, CatalogTransaction::GetSystemCatalogTransaction(context), path);
	if (caller_catalog &&
	    !SameStorageSecretRoute(selected.get(), LookupStorageSecret(context, *caller_catalog, path).get())) {
		throw TransactionException("Storage credentials changed before orphan cleanup");
	}
	return selected;
}

void OrphanPathNormalizer::RequireSameS3Route(const string &path) const {
	if (!SameStorageSecretRoute(s3_scan_secret.get(), ResolveStorageSecret(path).get())) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files cannot scan across different S3 storage routes");
	}
}

void OrphanPathNormalizer::ValidateS3Routes(const string &scan_root) {
	ValidateS3UriSuffix(scan_root);
	ValidateS3PathComponents(scan_root);
	if (scan_root.find_first_of("*[\\") != string::npos) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files requires an S3 scan prefix without glob characters");
	}
	s3_scan_prefix = DirectoryPrefix(scan_root);
	s3_scan_secret = ResolveStorageSecret(s3_scan_prefix + "**");
	if (s3_scan_secret && s3_scan_secret->secret->MatchScore(s3_scan_prefix) == NumericLimits<int64_t>::Minimum()) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files requires the S3 listing secret to cover the scan "
		    "prefix");
	}

	// Within the listing prefix, the selected secret can change only at a narrower scope.
	// Resolve those scopes once so shadowed secrets do not count as different storage routes.
	RequireSameS3Route(s3_scan_prefix);
	auto &manager = SecretManager::Get(context);
	unordered_set<string> scopes;
	auto collect_scopes = [&](CatalogTransaction transaction) {
		for (const auto &entry : manager.AllSecrets(transaction)) {
			if (!IsObjectStoreSecret(entry.secret->GetType())) {
				continue;
			}
			for (const auto &scope : entry.secret->GetScope()) {
				if (scope.size() > s3_scan_prefix.size() && StringUtil::StartsWith(scope, s3_scan_prefix)) {
					scopes.insert(scope);
				}
			}
		}
	};
	collect_scopes(CatalogTransaction::GetSystemCatalogTransaction(context));
	if (caller_catalog) {
		collect_scopes(*caller_catalog);
	}
	for (const auto &scope : scopes) {
		RequireSameS3Route(scope);
	}
	if (s3_scan_secret && IsS3Secret(s3_scan_secret->secret->GetType())) {
		Value use_global(true);
		context.TryGetCurrentSetting(S3_GLOBAL_CONFIGURATION, use_global);
		Value policy;
		if (s3_scan_secret->secret->Cast<KeyValueSecret>().TryGetValue(S3_GLOBAL_CONFIGURATION, policy) &&
		    !Value::NotDistinctFrom(policy, use_global)) {
			throw InvalidConfigurationException(
			    "iceberg_remove_orphan_files cannot mix S3 global configuration policies");
		}
	}
}

string OrphanPathNormalizer::ReferenceKey(const string &path, bool validate_file_reference) const {
	if (IsLocalOrphanPath(path)) {
		return NormalizeLocalOrphanPath(path);
	}
	auto scheme_end = path.find("://");
	auto scheme = StringUtil::Lower(path.substr(0, scheme_end));
	auto canonical_scheme = CanonicalS3Scheme(scheme, s3_aliases);
	if (!canonical_scheme) {
		throw InvalidInputException("iceberg_remove_orphan_files cannot verify a remote path with scheme '%s'", scheme);
	}
	if (validate_file_reference) {
		ValidateS3UriSuffix(path);
		ValidateReferenceRoute(path, canonical_scheme);
	}
	// These keys protect files across scheme aliases. Reads still use the original URI.
	return canonical_scheme + path.substr(scheme_end);
}

void OrphanPathNormalizer::ValidateReferenceRoute(const string &path, const char *scheme) const {
	ValidateS3PathComponents(path, true);
	if (!s3_scan_scheme || std::strcmp(scheme, s3_scan_scheme) != 0 || StringUtil::StartsWith(path, s3_scan_prefix)) {
		return;
	}
	// A bucket and an access point alias can address the same objects through the same secret.
	if (!StringUtil::CIEquals(S3Bucket(path), s3_scan_bucket)) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files requires S3 references to use the scan bucket: %s", path);
	}
	RequireSameS3Route(path);
}

const char *OrphanPathNormalizer::GetS3Scheme(const string &path) const {
	return IsLocalOrphanPath(path) ? nullptr
	                               : CanonicalS3Scheme(StringUtil::Lower(path.substr(0, path.find("://"))), s3_aliases);
}

OrphanFileReferences::OrphanFileReferences(const string &scan_key)
    : scan_scheme_prefix(IsLocalOrphanPath(scan_key) ? string() : scan_key.substr(0, scan_key.find("://") + 3)) {
}

string OrphanFileReferences::ReferenceAlias(const string &key) const {
	if (scan_scheme_prefix.empty() || !StringUtil::StartsWith(key, scan_scheme_prefix)) {
		return {};
	}
	const auto prefix_length = scan_scheme_prefix.size();
	const auto bucket_end = MinValue(key.find('/', prefix_length), key.size());
	const auto bucket = key.begin() + prefix_length;
	const auto end = key.begin() + bucket_end;
	if (std::none_of(bucket, end, [](char c) { return c >= 'A' && c <= 'Z'; })) {
		return {};
	}
	// Virtual-hosted URLs fold the bucket's case.
	auto alias = key;
	std::transform(bucket, end, alias.begin() + prefix_length, StringUtil::CharacterToLower);
	return alias;
}

void OrphanFileReferences::AddMetadataDirectory(string key) {
	auto alias = ReferenceAlias(key);
	metadata_directories.insert(alias.empty() ? std::move(key) : std::move(alias));
}

bool OrphanFileReferences::Add(const string &path, string key, bool count) {
	const bool comparable =
	    scan_scheme_prefix.empty() ? IsLocalOrphanPath(key) : StringUtil::StartsWith(key, scan_scheme_prefix);
	if (!comparable) {
		throw InvalidInputException("iceberg_remove_orphan_files cannot compare reference '%s' with the scan location",
		                            path);
	}
	auto &keys = count ? files : lookup_only;
	auto entry = keys.insert(std::move(key));
	if (!entry.second) {
		return false;
	}
	auto alias = ReferenceAlias(*entry.first);
	if (!alias.empty()) {
		lookup_only.insert(std::move(alias));
	}
	return true;
}

bool OrphanFileReferences::Contains(const string &key) const {
	if (files.count(key) || lookup_only.count(key)) {
		return true;
	}
	const auto alias = ReferenceAlias(key);
	if (!alias.empty() && (files.count(alias) || lookup_only.count(alias))) {
		return true;
	}
	// iceberg_scan accepts custom version hint names under the metadata directory.
	if (!StringUtil::EndsWith(key, ".text") && !StringUtil::EndsWith(key, ".txt")) {
		return false;
	}
	const auto &lookup = alias.empty() ? key : alias;
	return std::any_of(metadata_directories.begin(), metadata_directories.end(),
	                   [&](const string &directory) { return HasNormalizedPathPrefix(lookup, directory); });
}

static pair<idx_t, idx_t> GetLocalFileIdentity(const FileMetadata &metadata, const string &path) {
	if (!metadata.device_id.IsValid() || !metadata.file_id.IsValid()) {
		throw InvalidConfigurationException("iceberg_remove_orphan_files cannot verify local file identity for '%s'",
		                                    path);
	}
	return make_pair(metadata.device_id.GetIndex(), metadata.file_id.GetIndex());
}

void OrphanFileReferences::AddLocalFile(FileSystem &fs, const string &path) {
	auto metadata = fs.GetStatsIfExists(OpenFileInfo(path));
	if (metadata) {
		auto identity = GetLocalFileIdentity(*metadata, path);
		local_file_ids[identity.first].insert(identity.second);
	}
}

bool OrphanFileReferences::ContainsLocalFile(const FileMetadata &metadata, const string &path) const {
	if (local_file_ids.empty()) {
		return false;
	}
	auto identity = GetLocalFileIdentity(metadata, path);
	auto volume = local_file_ids.find(identity.first);
	return volume != local_file_ids.end() && volume->second.count(identity.second);
}

string JoinOrphanPath(FileSystem &fs, const string &directory, const string &child) {
#ifdef _WIN32
	if (IsLocalOrphanPath(directory)) {
		return fs.JoinPath(directory, child);
	}
#endif
	// FileSystem::JoinPath normalizes object keys and treats POSIX backslashes as separators.
	return DirectoryPrefix(directory) + child;
}

void RequireAbsoluteOrphanPath(const string &path, const char *kind) {
	if (!IsLocalOrphanPath(path)) {
		return;
	}
#ifdef _WIN32
	const bool absolute = Path::FromString(path).IsAbsolute();
#else
	const bool absolute = !path.empty() && path[0] == '/';
#endif
	if (!absolute) {
		throw InvalidConfigurationException("iceberg_remove_orphan_files requires an absolute %s path: %s", kind, path);
	}
}

void ValidateOrphanPath(const string &path) {
	if (HasParentDirectoryReference(path)) {
		throw InvalidConfigurationException("iceberg_remove_orphan_files: paths containing '..' are not supported: %s",
		                                    path);
	}
	if (!IsLocalOrphanPath(path)) {
		return;
	}
#ifdef _WIN32
	auto local_path = StringUtil::Replace(path, "\\", "/");
#else
	const auto &local_path = path;
#endif
	// Directory listings follow links in the pinned local file system. Check each
	// component so neither a scan root nor a retained reference can alias a file.
	for (idx_t end = 0; end <= local_path.size(); end++) {
		if (end != local_path.size() && local_path[end] != '/') {
			continue;
		}
		if (end && IsSymbolicLink(local_path.substr(0, end))) {
			throw InvalidConfigurationException(
			    "iceberg_remove_orphan_files: symbolic links in maintenance paths are not supported: %s", path);
		}
	}
}

bool HasParentDirectoryReference(const string &path) {
#ifdef _WIN32
	auto key = IsLocalOrphanPath(path) ? StringUtil::Replace(path, "\\", "/") : path;
#else
	const auto &key = path;
#endif
	const auto path_start = PathStart(key);
	for (auto pos = key.find("..", path_start); pos != string::npos; pos = key.find("..", pos + 2)) {
		if ((pos == path_start || key[pos - 1] == '/') && (pos + 2 == key.size() || key[pos + 2] == '/')) {
			return true;
		}
	}
	return false;
}

bool HasOrphanPathPrefix(const string &path, const string &prefix) {
	return HasNormalizedPathPrefix(NormalizeOrphanPath(path), NormalizeOrphanPath(prefix));
}

namespace {

bool TryGetExtendedTimestamp(const OpenFileInfo &info, timestamp_t &last_modified) {
	try {
		return info.extended_info && info.extended_info->TryGetOption("last_modified", last_modified);
	} catch (const InvalidInputException &) {
		return false;
	}
}

class OrphanScanner {
public:
	explicit OrphanScanner(const OrphanScannerInput &input)
	    : input(input), fs(FileSystem::GetFileSystem(input.context)) {
	}

	OrphanScanResult Scan() {
		input.context.InterruptCheck();
		D_ASSERT(!input.scan_root.empty());
		if (IsLocalOrphanPath(input.scan_root)) {
			ScanDirectoryTree(input.scan_root);
		} else {
			ScanS3Prefix(input.scan_root);
		}
		std::sort(result.candidates.begin(), result.candidates.end());
		result.candidates.erase(std::unique(result.candidates.begin(), result.candidates.end()),
		                        result.candidates.end());
		return std::move(result);
	}

private:
	void RequireRegisteredFileSystem(const char *name) {
		// Without a registered handler, the VFS dispatches to the local filesystem.
		auto registered = fs.ListSubSystems();
		if (std::find(registered.begin(), registered.end(), name) == registered.end()) {
			throw InvalidInputException("iceberg_remove_orphan_files: no file system registered to list %s",
			                            input.scan_root);
		}
	}

	void ScanS3Prefix(const string &root) {
		const auto prefix = DirectoryPrefix(root);
		RequireRegisteredFileSystem("S3FileSystem");
		const auto pattern = prefix + "**";
		const auto scheme = prefix.substr(0, prefix.find("://"));
		const auto scheme_length = scheme.size();
		const auto key_prefix = input.paths.ReferenceKey(prefix);
		// Route validation covers the whole prefix, including the paths returned by globbing.
		auto files = fs.Glob(pattern, FileGlobOptions::ALLOW_EMPTY, nullptr);
		for (const auto &info : files->Files()) {
			input.context.InterruptCheck();
			if (info.path == pattern && !info.extended_info) {
				throw InvalidConfigurationException("iceberg_remove_orphan_files requires S3 globbing to list files. "
				                                    "Disable s3_url_compatibility_mode for the scan location");
			}
			if (!StringUtil::CIStartsWith(info.path, scheme) ||
			    info.path.compare(scheme_length, prefix.size() - scheme_length, prefix, scheme_length, string::npos) !=
			        0) {
				throw InvalidConfigurationException(
				    "iceberg_remove_orphan_files: invalid listing result for S3 prefix %s: %s", prefix, info.path);
			}
			result.scanned_count++;
			// Skip listed keys that other clients could interpret as a different path.
			if (HasS3UriSuffix(info.path) || HasParentDirectoryReference(info.path) ||
			    HasAmbiguousPathComponents(info.path, true)) {
				continue;
			}
			const auto key = key_prefix + info.path.substr(prefix.size());
			if (StringUtil::StartsWith(info.path, scheme)) {
				ScanFile(info.path, key, info);
			} else {
				// Preserve the caller's scheme spelling for path-scoped secret lookup.
				ScanFile(scheme + info.path.substr(scheme_length), key, info);
			}
		}
		if (result.delete_candidates.empty()) {
			return;
		}

		// Count every occurrence, including newer or undated entries on another LIST page.
		for (const auto &info : files->Files()) {
			input.context.InterruptCheck();
			auto candidate = StringUtil::StartsWith(info.path, scheme)
			                     ? result.delete_candidates.find(info.path)
			                     : result.delete_candidates.find(scheme + info.path.substr(scheme_length));
			if (candidate != result.delete_candidates.end()) {
				candidate->second++;
			}
		}
	}

	void ScanDirectoryTree(const string &root) {
		vector<string> pending_directories {root};
		unordered_set<string> visited_directories;
		while (!pending_directories.empty()) {
			input.context.InterruptCheck();
			auto directory = std::move(pending_directories.back());
			pending_directories.pop_back();
			auto entry = visited_directories.insert(NormalizeOrphanPath(directory));
			if (!entry.second) {
				continue;
			}
			const auto &directory_key = *entry.first;
			// A queued directory may have changed since its parent was listed.
			ValidateOrphanPath(directory);
			auto visit_entry = [&](OpenFileInfo &info) {
				input.context.InterruptCheck();
				if (info.path.empty()) {
					return;
				}
				auto path = ResolveListedPath(fs, directory, info.path);
				const auto key = NormalizeOrphanPath(path);
				if (!HasNormalizedPathPrefix(key, directory_key)) {
					throw InvalidConfigurationException(
					    "iceberg_remove_orphan_files: listed path is outside its scan directory: %s", path);
				}
				ValidateOrphanPath(path);
				auto metadata = fs.GetStatsIfExists(OpenFileInfo(path));
				if (!metadata) {
					return;
				}
				if (metadata->file_type == FileType::FILE_TYPE_DIR) {
					pending_directories.push_back(std::move(path));
				} else if (metadata->file_type == FileType::FILE_TYPE_REGULAR) {
					result.scanned_count++;
					ScanFile(path, key, info, &*metadata);
				}
			};
			if (!fs.ListFiles(directory, visit_entry) &&
			    (directory != root || fs.GetStatsIfExists(OpenFileInfo(root)))) {
				throw IOException("iceberg_remove_orphan_files: could not list directory: %s", directory);
			}
		}
	}

	void ScanFile(const string &path, const string &key, const OpenFileInfo &info,
	              optional_ptr<const FileMetadata> metadata = nullptr) {
		if (input.referenced_files.Contains(key)) {
			return;
		}
		timestamp_t last_modified(0);
		if (metadata) {
			if (input.referenced_files.ContainsLocalFile(*metadata, path)) {
				return;
			}
			last_modified = metadata->last_modification_time;
		} else if (!TryGetExtendedTimestamp(info, last_modified) || last_modified.value <= 0) {
			return;
		}
		if (!last_modified.IsFinite() || last_modified >= input.older_than) {
			return;
		}
		if (input.for_deletion) {
			auto &observations = result.delete_candidates[path];
			if (metadata) {
				// Local entries have no listing to replay.
				observations++;
			}
		} else {
			result.candidates.push_back(path);
		}
	}

private:
	const OrphanScannerInput &input;
	FileSystem &fs;
	OrphanScanResult result;
};

} // namespace

OrphanScanResult FindOrphanFiles(const OrphanScannerInput &input) {
	OrphanScanner scanner(input);
	return scanner.Scan();
}

} // namespace duckdb
