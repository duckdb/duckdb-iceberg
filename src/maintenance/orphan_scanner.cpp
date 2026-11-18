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
#include "catalog/rest/iceberg_catalog.hpp"

#include <algorithm>
#include <cstring>
#include <functional>
#include <iterator>
#include <typeinfo>

#ifdef _WIN32
#include "duckdb/common/path.hpp"
#include "duckdb/common/windows.hpp"
#else
#include <cerrno>
#include <dirent.h>
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

bool IsAzureDfsPath(const string &path) {
	return StringUtil::StartsWith(path, "abfs://") || StringUtil::StartsWith(path, "abfss://");
}

constexpr const char AZURE_URI_PREFIX[] = "azure://";
constexpr idx_t AZURE_URI_PREFIX_LENGTH = sizeof(AZURE_URI_PREFIX) - 1;

string AzureReferenceKey(const string &path, idx_t scheme_end, bool container_at_account) {
	auto authority_start = scheme_end + 3;
	auto authority_end = path.find('/', authority_start);
	if (authority_end == string::npos) {
		return AZURE_URI_PREFIX + path.substr(authority_start);
	}
	auto authority = path.substr(authority_start, authority_end - authority_start);
	auto dot = authority.find('.');
	auto at = authority.find('@');
	if (container_at_account && at < dot && dot != string::npos) {
		// Azure accepts both container@account.endpoint and account.endpoint/container.
		return AZURE_URI_PREFIX + StringUtil::Lower(authority.substr(at + 1)) + "/" + authority.substr(0, at) +
		       path.substr(authority_end);
	}
	return AZURE_URI_PREFIX + (dot == string::npos ? authority : StringUtil::Lower(authority)) +
	       path.substr(authority_end);
}

string AzureReferenceAlias(const string &key) {
	if (!StringUtil::StartsWith(key, AZURE_URI_PREFIX)) {
		return {};
	}
	const auto slash = key.find('/', AZURE_URI_PREFIX_LENGTH);
	if (slash == string::npos) {
		return {};
	}
	// A connection string can override the endpoint in the URI. Match the container and literal key.
	const bool has_endpoint = key.find('.', AZURE_URI_PREFIX_LENGTH) < slash;
	if (!has_endpoint && key.back() != '/') {
		return {};
	}
	auto alias = key.substr(has_endpoint ? slash + 1 : AZURE_URI_PREFIX_LENGTH);
	// DFS and Hadoop FileIO can resolve a reference with a trailing slash to the same file.
	if (!alias.empty() && alias.back() == '/') {
		alias.pop_back();
	}
	return AZURE_URI_PREFIX + alias;
}

string S3ReferenceAlias(const string &key, const string &scan_prefix) {
	if (!StringUtil::StartsWith(key, scan_prefix)) {
		return {};
	}
	const auto prefix_length = scan_prefix.size();
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

void ValidateRemotePathComponents(const string &path, const char *kind, bool require_file_key = false) {
	if (HasAmbiguousPathComponents(path, require_file_key)) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files cannot verify %s with empty or '.' path components: %s", kind, path);
	}
}

void ValidateObjectUriSuffix(const string &path, const char *backend) {
	// Iceberg FileIO implementations can treat these suffixes as URI syntax.
	if (path.find_first_of("?#") != string::npos) {
		throw InvalidConfigurationException("iceberg_remove_orphan_files cannot verify %s paths containing '?' or '#'",
		                                    backend);
	}
}

void ValidateAzurePath(const string &path) {
	ValidateObjectUriSuffix(path, "Azure");
	ValidateRemotePathComponents(path, "Azure paths");
}

void ValidateAzureScanRoot(const string &root) {
	if (StringUtil::StartsWith(root, "az://") || StringUtil::StartsWith(root, "azure://") ||
	    StringUtil::StartsWith(root, "wasb://") || StringUtil::StartsWith(root, "wasbs://")) {
		throw InvalidInputException("iceberg_remove_orphan_files does not support Azure Blob scan locations");
	}
	if (!IsAzureDfsPath(root)) {
		return;
	}
	ValidateAzurePath(root);
	const auto key = AzureReferenceKey(root, root.find("://"), true);
	const auto authority_end = PathStart(key);
	auto directory_start = authority_end;
	if (key.find('.', AZURE_URI_PREFIX_LENGTH) < authority_end) {
		// A fully qualified URI puts the container after the authority.
		directory_start = key.find('/', authority_end + 1);
	}
	// The pinned DFS driver strips the first character of each listed name at the container root.
	if (authority_end == AZURE_URI_PREFIX_LENGTH || directory_start == string::npos ||
	    key.find_first_not_of('/', directory_start) == string::npos) {
		throw InvalidInputException(
		    "iceberg_remove_orphan_files requires a nonempty directory in Azure DFS scan locations");
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

unique_ptr<SecretEntry> LookupStorageSecret(ClientContext &context, const string &path, bool azure) {
	auto &manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (azure) {
		return manager.LookupSecret(transaction, path, "azure").secret_entry;
	}
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
	return IsS3Secret(type) || type == "http" || type == "azure";
}

bool SameStorageSecretRoute(optional_ptr<const SecretEntry> left, optional_ptr<const SecretEntry> right) {
	if (!left || !right) {
		return !left && !right;
	}
	const auto &a = left->secret->Cast<KeyValueSecret>();
	const auto &b = right->secret->Cast<KeyValueSecret>();
	if (a.GetType() != b.GetType() || a.GetProvider() != b.GetProvider()) {
		return false;
	}
	if (left->storage_mode != right->storage_mode || a.GetName() != b.GetName()) {
		if (a.GetType() == "azure" || a.GetProvider() != "config" || a.secret_map.count("refresh_info") ||
		    b.secret_map.count("refresh_info")) {
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

string ResolveListedPath(FileSystem &fs, const string &directory, const string &listed_path, bool local) {
	if (fs.IsPathAbsolute(listed_path) || (!local && HasUriScheme(listed_path))) {
		return fs.ExpandPath(listed_path);
	}
	return JoinOrphanPath(fs, directory, listed_path);
}

string NormalizeOrphanPath(const string &path) {
	// Preserve remote schemes and key components, including repeated slashes and '.'.
	if (!IsLocalOrphanPath(path)) {
		return path;
	}
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

bool HasNormalizedPathPrefix(const string &path, const string &prefix) {
	if (path == prefix) {
		return true;
	}
	return !prefix.empty() && StringUtil::StartsWith(path, prefix) &&
	       (prefix.back() == '/' || path[prefix.size()] == '/');
}

class OrphanFileSystem final : public OpenerFileSystem {
public:
	explicit OrphanFileSystem(unique_ptr<OpenerFileSystem> fs) : fs(std::move(fs)) {
	}

	FileSystem &GetFileSystem() const override {
		return fs->GetFileSystem();
	}

	optional_ptr<FileOpener> GetOpener() const override {
		return fs->GetOpener();
	}

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override {
		// Avro requests caching even though the shared cache keys files only by path.
		flags.SetCachingMode(CachingMode::NO_CACHING);
		auto context = IsLocalOrphanPath(file.path) ? nullptr : FileOpener::TryGetClientContext(GetOpener());
		if (context) {
			// A listing can hold on to the prior HTTPFS state.
			context->registered_state->Remove("http_state");
		}
		auto handle = fs->OpenFile(file, flags, opener);
		if (context) {
			// The open handle owns its HTTP download state. Release the private connection's reference.
			context->registered_state->Remove("http_state");
		}
		return handle;
	}

private:
	unique_ptr<OpenerFileSystem> fs;
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
	ValidateAzureScanRoot(root);
	// The caller owns the catalog transaction and its credential callback. File I/O has a private
	// settings map and transaction, and follows cancellation of the calling query.
	auto connection = Connection::CreateInternal(caller);
	auto &io = *connection.context;
	io.config.user_settings = caller.config.user_settings;
	io.logger = caller.logger;
	auto &original_fs = FileSystem::GetFileSystem(caller);
	auto &io_fs = ClientData::Get(io).client_file_system;
	auto original = dynamic_cast<OpenerFileSystem *>(&original_fs);
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
	auto visible = manager.AllSecrets(source);
	for (const auto &entry : visible) {
		const bool transaction_secret = entry.persist_type == SecretPersistType::TRANSACTION;
		if (transaction_secret || entry.storage_mode == SecretManager::CONNECTION_STORAGE_NAME) {
			manager.RegisterSecret(destination, entry.secret->Clone(), OnCreateConflict::ERROR_ON_CONFLICT,
			                       entry.persist_type, transaction_secret ? string() : entry.storage_mode);
		}
	}

	// A dry-run can be inside a transaction that created, replaced or dropped a global secret.
	// Check snapshot visibility before a different set of storage credentials can be used.
	auto &system_catalog = Catalog::GetSystemCatalog(caller);
	if (system_catalog.GetCatalogVersion(caller) != system_catalog.GetCatalogVersion(io)) {
		auto shared_storage = [](const SecretEntry &entry) {
			return IsObjectStoreSecret(entry.secret->GetType()) &&
			       entry.persist_type != SecretPersistType::TRANSACTION &&
			       entry.storage_mode != SecretManager::CONNECTION_STORAGE_NAME;
		};
		auto expected = std::count_if(visible.begin(), visible.end(), shared_storage);
		for (const auto &entry : manager.AllSecrets(destination)) {
			if (!shared_storage(entry)) {
				continue;
			}
			auto before =
			    manager.GetSecretByName(source, entry.secret->GetName().GetIdentifierName(), entry.storage_mode);
			if (!SameStorageSecretRoute(before.get(), &entry) ||
			    before->secret->GetScope() != entry.secret->GetScope()) {
				throw TransactionException("Storage credentials changed before orphan cleanup");
			}
			--expected;
		}
		if (expected != 0) {
			throw TransactionException("Storage credentials changed before orphan cleanup");
		}
	}

	for (const auto &path : {root, metadata_location}) {
		if (!IsLocalOrphanPath(path)) {
			ExtensionHelper::AutoLoadExtensionForPath(*caller.db, path);
		}
	}
	// Vended credentials belong to this operation. Parallel scans of the same caller do
	// not install or refresh each other's transaction secrets.
	table.LoadCredentials(io);

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
	// HTTPFS ignores global S3 fallbacks when disabled. Do not promote those values to session settings.
	for (const auto *name : {"s3_endpoint", "s3_region", "s3_url_style", "s3_use_ssl", "s3_use_global_endpoint",
	                         "s3_url_compatibility_mode", "s3_access_key_id", "s3_secret_access_key",
	                         "s3_session_token", "s3_requester_pays", "s3_kms_key_id", "gcs_user_project"}) {
		Value value;
		auto scope = caller.TryGetCurrentSetting(name, value);
		if (scope && scope.GetScope() == SettingScope::GLOBAL && !allow_global) {
			value = Value();
		}
		set(name, std::move(value));
	}
	set(S3_GLOBAL_CONFIGURATION, use_global);
	for (const auto *name : {"merge_http_secret_into_s3_request", "extra_http_headers", "http_proxy",
	                         "http_proxy_username", "http_proxy_password", "azure_storage_connection_string",
	                         "azure_account_name", "azure_endpoint", "azure_credential_chain"}) {
		Value value;
		if (!caller.TryGetCurrentSetting(name, value) && StringUtil::StartsWith(name, "azure_")) {
			value = Value("");
		}
		set(name, std::move(value));
	}
	set("home_directory", Value(original_fs.GetHomeDirectory()));
	set("httpfs_enable_credential_refresh", Value(false));
	set("enable_http_metadata_cache", Value(false));
	if (for_deletion && !IsLocalOrphanPath(root)) {
		// Rechecking an orphan's metadata should not download its contents.
		set("force_download", Value(false));
		set("force_download_threshold", Value::UBIGINT(0));
		set("auto_fallback_to_full_download", Value(false));
		set("http_retries", Value::UBIGINT(0));
	}
	return connection;
}

OrphanPathNormalizer::OrphanPathNormalizer(ClientContext &context, const string &scan_root) {
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
		s3_scan_bucket = S3Bucket(scan_root);
		ValidateS3Routes(context, scan_root);
	}
}

OrphanPathNormalizer::~OrphanPathNormalizer() = default;

void OrphanPathNormalizer::ValidateS3Routes(ClientContext &context, const string &scan_root) {
	ValidateObjectUriSuffix(scan_root, "S3");
	ValidateRemotePathComponents(scan_root, "S3 object keys");
	if (scan_root.find_first_of("*[\\") != string::npos) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files requires an S3 scan prefix without glob characters");
	}
	const auto prefix = DirectoryPrefix(scan_root);
	s3_scan_secret = LookupStorageSecret(context, prefix + "**", false);
	if (s3_scan_secret && s3_scan_secret->secret->MatchScore(prefix) == NumericLimits<int64_t>::Minimum()) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files requires the S3 listing secret to cover the scan "
		    "prefix");
	}

	auto &manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto could_match = [&](const BaseSecret &secret) {
		const auto &scopes = secret.GetScope();
		return scopes.empty() || std::any_of(scopes.begin(), scopes.end(), [&](const string &scope) {
			       // Partial scheme prefixes (including an empty scope) can match any path in this backend.
			       const auto scheme_end = scope.find("://");
			       if (scheme_end == string::npos) {
				       return true;
			       }
			       const auto *scheme = GetS3Scheme(scope);
			       if (!scheme || std::strcmp(scheme, s3_scan_scheme) != 0) {
				       return false;
			       }
			       const auto bucket_start = scheme_end + 3;
			       const auto bucket_end = scope.find('/', bucket_start);
			       // A slash after the authority proves the scope is confined to that bucket.
			       return bucket_end == string::npos ||
			              StringUtil::CIEquals(scope.substr(bucket_start, bucket_end - bucket_start), s3_scan_bucket);
		       });
	};
	Value merge_http(true);
	context.TryGetCurrentSetting("merge_http_secret_into_s3_request", merge_http);
	const bool http_fallback = (merge_http.IsNull() || merge_http.GetValue<bool>()) &&
	                           (!s3_scan_secret || s3_scan_secret->secret->GetType() == "http");
	// The scan and references in its bucket must resolve equivalent routes across scheme aliases.
	for (const auto &entry : manager.AllSecrets(transaction)) {
		const auto &type = entry.secret->GetType();
		if ((IsS3Secret(type) || (http_fallback && type == "http")) && could_match(*entry.secret) &&
		    !SameStorageSecretRoute(s3_scan_secret.get(), &entry)) {
			throw InvalidConfigurationException(
			    "iceberg_remove_orphan_files cannot scan across different S3 storage routes");
		}
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

string OrphanPathNormalizer::ReferenceKey(const string &path) const {
	auto key = NormalizeOrphanPath(path);
	if (IsLocalOrphanPath(key)) {
		return key;
	}
	auto scheme_end = key.find("://");
	auto scheme = StringUtil::Lower(key.substr(0, scheme_end));
	auto canonical_scheme = CanonicalS3Scheme(scheme, s3_aliases);
	const bool dfs = scheme == "abfs" || scheme == "abfss";
	const bool wasb = !canonical_scheme && (scheme == "wasb" || scheme == "wasbs");
	if (dfs || wasb || scheme == "az" || scheme == "azure") {
		ValidateAzurePath(path);
		return AzureReferenceKey(key, scheme_end, dfs || wasb);
	}
	if (!canonical_scheme) {
		throw InvalidInputException("iceberg_remove_orphan_files cannot verify a remote path with scheme '%s'", scheme);
	}
	ValidateObjectUriSuffix(path, "S3");
	// These keys protect files across scheme aliases. Reads still use the original URI.
	key.replace(0, scheme_end, canonical_scheme);
	return key;
}

void OrphanPathNormalizer::ValidateReferenceRoute(const string &path) const {
	const auto *scheme = GetS3Scheme(path);
	if (!scheme) {
		return;
	}
	ValidateRemotePathComponents(path, "S3 object keys", true);
	if (!s3_scan_scheme || std::strcmp(scheme, s3_scan_scheme) != 0) {
		return;
	}
	// A bucket and an access point alias can address the same objects through the same secret.
	if (!StringUtil::CIEquals(S3Bucket(path), s3_scan_bucket)) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files requires S3 references to use the scan bucket: %s", path);
	}
	if (s3_scan_secret && s3_scan_secret->secret->MatchScore(path) == NumericLimits<int64_t>::Minimum()) {
		throw InvalidConfigurationException("iceberg_remove_orphan_files cannot verify an S3 path outside the scan "
		                                    "secret's scope: %s",
		                                    path);
	}
}

const char *OrphanPathNormalizer::GetS3Scheme(const string &path) const {
	return IsLocalOrphanPath(path) ? nullptr
	                               : CanonicalS3Scheme(StringUtil::Lower(path.substr(0, path.find("://"))), s3_aliases);
}

OrphanFileReferences::OrphanFileReferences(const string &scan_key)
    : scan_scheme_prefix(IsLocalOrphanPath(scan_key) ? string() : scan_key.substr(0, scan_key.find("://") + 3)) {
}

string OrphanFileReferences::ReferenceAlias(const string &key) const {
	if (scan_scheme_prefix == AZURE_URI_PREFIX) {
		return AzureReferenceAlias(key);
	}
	return scan_scheme_prefix.empty() ? string() : S3ReferenceAlias(key, scan_scheme_prefix);
}

void OrphanFileReferences::AddMetadataDirectory(string key) {
	auto alias = ReferenceAlias(key);
	if (!alias.empty()) {
		metadata_directories.push_back(std::move(alias));
	}
	metadata_directories.push_back(std::move(key));
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
	return std::any_of(metadata_directories.begin(), metadata_directories.end(), [&](const string &directory) {
		return HasNormalizedPathPrefix(key, directory) || (!alias.empty() && HasNormalizedPathPrefix(alias, directory));
	});
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

void ValidateOrphanPath(const string &path, bool parent_verified) {
	if (!parent_verified && HasParentDirectoryReference(path)) {
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
	for (idx_t end = parent_verified ? local_path.size() : 0; end <= local_path.size(); end++) {
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

#ifndef _WIN32
bool TryListPosixDirectory(FileSystem &fs, const string &directory,
                           const std::function<void(OpenFileInfo &, optional_ptr<const FileMetadata>)> &callback) {
	// Use the native iterator only when the session filesystem resolves this exact local directory.
	auto context_fs = dynamic_cast<OpenerFileSystem *>(&fs);
	if (!context_fs || !fs.DirectoryExists(directory)) {
		return false;
	}
	// DirectoryExists applies directory permissions. The identity probe uses the same routed filesystem.
	auto directory_info =
	    context_fs->GetFileSystem().GetStatsIfExists(OpenFileInfo(directory), context_fs->GetOpener());
	const auto prefix = DirectoryPrefix(directory);
	if (!directory_info || directory_info->file_type != FileType::FILE_TYPE_DIR ||
	    !directory_info->device_id.IsValid() || !directory_info->file_id.IsValid()) {
		return false;
	}
	unique_ptr<DIR, void (*)(DIR *)> handle(opendir(directory.c_str()), [](DIR *dir) { closedir(dir); });
	struct stat status;
	if (!handle || fstat(dirfd(handle.get()), &status) != 0 ||
	    directory_info->device_id.GetIndex() != static_cast<idx_t>(status.st_dev) ||
	    directory_info->file_id.GetIndex() != static_cast<idx_t>(status.st_ino)) {
		return false;
	}
	while (true) {
		errno = 0;
		auto entry = readdir(handle.get());
		if (!entry) {
			if (errno != 0) {
				const auto error = errno;
				throw IOException({{"errno", std::to_string(error)}},
				                  "iceberg_remove_orphan_files: could not list directory %s: %s", directory,
				                  std::strerror(error));
			}
			break;
		}
		if (std::strcmp(entry->d_name, ".") == 0 || std::strcmp(entry->d_name, "..") == 0) {
			continue;
		}
		// Stat the literal path through the session filesystem and keep its access checks.
		OpenFileInfo info(prefix + entry->d_name);
		auto metadata = fs.GetStatsIfExists(info);
		if (!metadata ||
		    (metadata->file_type != FileType::FILE_TYPE_DIR && metadata->file_type != FileType::FILE_TYPE_REGULAR)) {
			continue;
		}
		callback(info, &*metadata);
	}
	return true;
}
#endif

bool TryGetExtendedTimestamp(const OpenFileInfo &info, timestamp_t &last_modified) {
	try {
		return info.extended_info && info.extended_info->TryGetOption("last_modified", last_modified) &&
		       last_modified.IsFinite() && last_modified.value > 0;
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
		if (input.paths.GetS3Scheme(input.scan_root)) {
			ScanS3Prefix(input.scan_root);
		} else if (IsAzureDfsPath(input.scan_root)) {
			RequireRegisteredFileSystem("AzureDfsStorageFileSystem");
			azure_listing_secret = LookupStorageSecret(input.context, input.scan_root, true);
			ScanDirectoryTree(input.scan_root);
			ValidateAzureCandidates();
		} else if (!IsLocalOrphanPath(input.scan_root)) {
			throw InvalidInputException("iceberg_remove_orphan_files: unsupported remote scan location %s",
			                            input.scan_root);
		} else {
			ScanDirectoryTree(input.scan_root);
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
			if (StringUtil::StartsWith(info.path, scheme)) {
				ScanFile(info.path, info);
			} else {
				// Preserve the caller's scheme spelling for path-scoped secret lookup.
				ScanFile(scheme + info.path.substr(scheme_length), info);
			}
		}
	}

	void ScanDirectoryTree(const string &root) {
		const bool local = IsLocalOrphanPath(root);
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
			if (local && directory == root && !fs.DirectoryExists(root)) {
				return;
			}
			if (!local && directory != root) {
				ValidateAzurePath(directory);
				if (!SameStorageSecretRoute(azure_listing_secret.get(),
				                            LookupStorageSecret(input.context, directory, true).get())) {
					throw InvalidConfigurationException(
					    "iceberg_remove_orphan_files: Azure directories select different path-based secret routes");
				}
			}
			auto visit_entry = [&](OpenFileInfo &info, optional_ptr<const FileMetadata> metadata) {
				input.context.InterruptCheck();
				if (info.path.empty()) {
					return;
				}
				auto path = metadata ? info.path : ResolveListedPath(fs, directory, info.path, local);
				if (!metadata && !HasNormalizedPathPrefix(NormalizeOrphanPath(path), directory_key)) {
					throw InvalidConfigurationException(
					    "iceberg_remove_orphan_files: listed path is outside its scan directory: %s", path);
				}
				if (metadata ? metadata->file_type == FileType::FILE_TYPE_DIR : fs.IsDirectory(info)) {
					// Finish this listing before opening any child directory.
					pending_directories.push_back(std::move(path));
					return;
				}
				ScanFile(path, info, metadata);
			};
#ifndef _WIN32
			if (local && TryListPosixDirectory(fs, directory, visit_entry)) {
				continue;
			}
#endif
			// Azure returns false for a successful empty listing and throws on request failures.
			if (!fs.ListFiles(directory, [&](OpenFileInfo &info) { visit_entry(info, nullptr); }) && local) {
				throw IOException("iceberg_remove_orphan_files: could not list directory: %s", directory);
			}
		}
	}

	void ScanFile(const string &path, const OpenFileInfo &info, optional_ptr<const FileMetadata> metadata = nullptr) {
		// Native entries join a literal basename to a verified parent.
		ValidateOrphanPath(path, bool(metadata));
		result.scanned_count++;
		const bool local = IsLocalOrphanPath(path);
		if (!local && HasAmbiguousPathComponents(path, true)) {
			return;
		}
		if (input.referenced_files.Contains(input.paths.ReferenceKey(path))) {
			return;
		}
		timestamp_t last_modified(0);
		const bool has_listing_time = !metadata && TryGetExtendedTimestamp(info, last_modified);
		optional<FileMetadata> local_stats;
		if (local && !metadata) {
			local_stats = fs.GetStatsIfExists(OpenFileInfo(path));
			metadata = local_stats ? &*local_stats : nullptr;
		}
		if (metadata) {
			if (input.referenced_files.ContainsLocalFile(*metadata, path)) {
				return;
			}
			if (!has_listing_time) {
				last_modified = metadata->last_modification_time;
			}
		}
		if (!last_modified.IsFinite() || last_modified.value <= 0 || last_modified >= input.older_than) {
			return;
		}
		if (input.for_deletion) {
			OrphanFileVersion version {{}, -1, last_modified};
			if (local) {
				if (metadata && metadata->file_type == FileType::FILE_TYPE_REGULAR && metadata->device_id.IsValid() &&
				    metadata->file_id.IsValid() && metadata->last_modification_time == last_modified) {
					version.tag = metadata->version_tag;
					version.size = metadata->file_size;
				}
			} else if (info.extended_info) {
				info.extended_info->TryGetOption("etag", version.tag);
				idx_t size;
				if (info.extended_info->TryGetOption("file_size", size)) {
					version.size = NumericCast<int64_t>(size);
				}
			}
			auto entry = result.delete_candidates.emplace(path, std::move(version));
			if (!entry.second) {
				// A repeated listing does not establish a unique observation.
				entry.first->second.tag.clear();
			}
		} else {
			result.candidates.push_back(path);
		}
	}

private:
	void ValidateAzureCandidates() const;
	const OrphanScannerInput &input;
	FileSystem &fs;
	OrphanScanResult result;
	unique_ptr<SecretEntry> azure_listing_secret;
};

void OrphanScanner::ValidateAzureCandidates() const {
	auto &context = input.context;
	if (result.candidates.empty()) {
		return;
	}
	if (!SameStorageSecretRoute(azure_listing_secret.get(),
	                            LookupStorageSecret(context, input.scan_root, true).get())) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files: the Azure listing route changed during cleanup");
	}
	for (const auto &path : result.candidates) {
		context.InterruptCheck();
		auto selected = LookupStorageSecret(context, path, true);
		if (!SameStorageSecretRoute(azure_listing_secret.get(), selected.get())) {
			throw InvalidConfigurationException(
			    "iceberg_remove_orphan_files: Azure listing and file access select different path-based secret routes");
		}
	}
}

} // namespace

OrphanScanResult FindOrphanFiles(const OrphanScannerInput &input) {
	OrphanScanner scanner(input);
	return scanner.Scan();
}

} // namespace duckdb
