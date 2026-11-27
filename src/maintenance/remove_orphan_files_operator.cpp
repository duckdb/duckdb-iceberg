#include "maintenance/remove_orphan_files_operator.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_file_credentials.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "iceberg_options.hpp"
#include "maintenance/maintenance_table_loader.hpp"
#include "maintenance/orphan_scanner.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Referenced file collection
//===--------------------------------------------------------------------===//

string ExpandStoredOrphanPath(FileSystem &fs, const string &path, const char *kind) {
	if (StringUtil::StartsWith(path, "file:/") && path.find_first_of("?#%") != string::npos) {
		throw InvalidConfigurationException(
		    "iceberg_remove_orphan_files cannot verify a file URI with a query, fragment or percent sign");
	}
	// A path persisted by the writer must not depend on the cleanup session's home directory.
	auto expanded = StringUtil::StartsWith(path, "~") ? path : fs.ExpandPath(path);
	RequireAbsoluteOrphanPath(expanded, kind);
	return expanded;
}

class ReferencedFileCollector {
public:
	ReferencedFileCollector(ClientContext &context, const string &table_location, const OrphanPathNormalizer &paths,
	                        const IcebergFileSecret &file_secret, OrphanFileReferences &references)
	    : table_location(table_location), fs(FileSystem::GetFileSystem(context)), context(context), paths(paths),
	      file_secret(file_secret), references(references) {
	}

	const char *CollectFromMetadataFile(const string &path, const rest_api_objects::TableMetadata &catalog,
	                                    const IcebergTableMetadata &embedded, bool scan_manifests);

	void CollectMetadataFiles(const rest_api_objects::TableMetadata &source) {
		if (source.properties) {
			auto dir = source.properties->find("write.metadata.path");
			if (dir != source.properties->end()) {
				ProtectMetadataDirectory(ExpandStoredOrphanPath(fs, dir->second, "metadata directory"));
			}
		}
		if (source.snapshots) {
			for (const auto &snapshot : *source.snapshots) {
				if (snapshot.manifest_list) {
					AddReference(*snapshot.manifest_list, "manifest list");
				} else if (snapshot.manifests) {
					for (const auto &path : *snapshot.manifests) {
						AddReference(path, "manifest");
					}
				}
			}
		}
		if (source.metadata_log) {
			for (const auto &item : source.metadata_log->value) {
				AddReference(item.metadata_file, "metadata log");
			}
		}
		if (source.statistics) {
			for (const auto &stats : *source.statistics) {
				AddReference(stats.statistics_path, "statistics");
			}
		}
		if (source.partition_statistics) {
			for (const auto &stats : *source.partition_statistics) {
				AddReference(stats.statistics_path, "partition statistics");
			}
		}
	}

private:
	void ProtectMetadataDirectory(const string &directory) {
		AddReference(JoinOrphanPath(fs, directory, DEFAULT_VERSION_HINT_FILE), "version hint", false);
		references.AddMetadataDirectory(paths.ReferenceKey(directory));
	}

	void AddReference(const string &path, const char *kind, bool count = true) {
		const auto expanded = ExpandStoredOrphanPath(fs, path, kind);
		auto key = paths.ReferenceKey(expanded, true);
		if (!references.Add(expanded, std::move(key), count)) {
			return;
		}
		ValidateOrphanPath(expanded);
		if (IsLocalOrphanPath(expanded)) {
			references.AddLocalFile(fs, expanded);
		}
	}

	void CollectSnapshots(const IcebergTableMetadata &metadata, const vector<rest_api_objects::Snapshot> &snapshots) {
		unordered_set<string> manifest_lists;
		unordered_set<string> manifest_keys;
		auto already_collected = [&](const IcebergManifestListEntry &manifest_file) {
			const auto &file = manifest_file.GetFile();
			// The original URI selects the secret and endpoint for this read.
			auto key = StringUtil::Format(
			    "%llu|%s|%d|%d|%lld", static_cast<unsigned long long>(file.manifest_path.size()), file.manifest_path,
			    static_cast<int32_t>(file.partition_spec_id), static_cast<int32_t>(file.content), file.manifest_length);
			if (!manifest_keys.insert(std::move(key)).second) {
				return true;
			}
			// The manifest entries determine reachability even when the list counts are inconsistent.
			AddReference(file.manifest_path, "manifest");
			return false;
		};
		for (const auto &entry : snapshots) {
			if (entry.manifest_list) {
				if (!manifest_lists.insert(*entry.manifest_list).second) {
					continue;
				}
			} else if (metadata.iceberg_version == 1 && entry.manifests && entry.manifests->empty()) {
				// An explicit empty v1 inline list contributes no files. Other retained snapshots still do.
				continue;
			}
			const auto &snapshot = *metadata.GetSnapshotById(entry.snapshot_id);
			vector<IcebergManifestListEntry> manifest_files;
			IcebergManifestList::LoadManifestFiles({snapshot, metadata.GetCurrentSchemaId()}, metadata, context,
			                                       manifest_files, file_secret);
			auto end = std::remove_if(manifest_files.begin(), manifest_files.end(), already_collected);
			manifest_files.erase(end, manifest_files.end());
			if (!manifest_files.empty()) {
				manifest_file::ManifestReader::ReadPaths(
				    manifest_files, metadata, context, file_secret,
				    [&](const string &path) { AddReference(path, "data or delete file"); });
			}
		}
	}

private:
	const string &table_location;
	FileSystem &fs;
	ClientContext &context;
	const OrphanPathNormalizer &paths;
	const IcebergFileSecret &file_secret;
	OrphanFileReferences &references;
};

//===--------------------------------------------------------------------===//
// Error handling and result rows
//===--------------------------------------------------------------------===//

static constexpr const char *REMOVE_ORPHAN_FILES_NAME = "iceberg_remove_orphan_files";
static constexpr const char *SUCCESS = "SUCCESS";
static constexpr const char *GC_DISABLED = "GC_DISABLED";
static constexpr const char *PREFIX_MISMATCH = "PREFIX_MISMATCH";
static constexpr const char *MISSING_METADATA_LOCATION = "MISSING_METADATA_LOCATION";
static constexpr int64_t DEFAULT_RETENTION_MICROS = 3 * Interval::MICROS_PER_DAY;

vector<LogicalType> RemoveOrphanFilesResultTypes() {
	return {LogicalType::LIST(LogicalType::VARCHAR),
	        LogicalType::BIGINT,
	        LogicalType::BIGINT,
	        LogicalType::BIGINT,
	        LogicalType::VARCHAR,
	        LogicalType::LIST(LogicalType::VARCHAR),
	        LogicalType::BIGINT};
}

void WriteFileList(Vector &list, const vector<string> &files) {
	auto writer = FlatVector::Writer<VectorListType<string_t>>(list, 1);
	auto entries = writer.WriteList(files.size());
	for (const auto &path : files) {
		entries.child_writer.WriteValue(string_t(path));
	}
}

struct RemoveOrphanFilesResult {
	RemoveOrphanFilesResult() = default;

	explicit RemoveOrphanFilesResult(const char *error_code) : error_code(error_code) {
	}

	vector<string> file_locations;
	vector<string> failed_file_locations;
	int64_t scanned_files = 0;
	int64_t referenced_files = 0;
	const char *error_code = SUCCESS;
};

//===--------------------------------------------------------------------===//
// Execute
//===--------------------------------------------------------------------===//

bool GCEnabled(const IcebergTableMetadata &metadata) {
	const auto &properties = metadata.GetTableProperties();
	auto entry = properties.find("gc.enabled");
	if (entry == properties.end() || StringUtil::CIEquals(entry->second, "true")) {
		return true;
	}
	if (StringUtil::CIEquals(entry->second, "false")) {
		return false;
	}
	throw InvalidConfigurationException("%s: invalid value '%s' for table property 'gc.enabled': "
	                                    "expected 'true' or 'false'",
	                                    REMOVE_ORPHAN_FILES_NAME, entry->second);
}

bool CatalogMatchesMetadataFile(const rest_api_objects::TableMetadata &left,
                                const rest_api_objects::TableMetadata &right) {
	return left.table_uuid == right.table_uuid && left.location == right.location &&
	       left.last_updated_ms == right.last_updated_ms &&
	       left.current_snapshot_id.value_or(-1) == right.current_snapshot_id.value_or(-1);
}

void RequireUniqueSnapshotIds(const rest_api_objects::TableMetadata &source, const IcebergTableMetadata &parsed) {
	// The parsed map retains only one manifest list per snapshot ID.
	if (source.snapshots && source.snapshots->size() != parsed.snapshots.size()) {
		throw InvalidConfigurationException("Duplicate snapshot ID in Iceberg table metadata");
	}
}

void ValidateRetainedSnapshots(const IcebergTableMetadata &file, const IcebergTableMetadata &catalog) {
	// LoadTable may omit snapshots. Every snapshot it advertises must agree with the metadata file.
	for (const auto &entry : catalog.snapshots) {
		auto retained = file.snapshots.find(entry.first);
		if (retained == file.snapshots.end() || retained->second.manifest_list != entry.second.manifest_list ||
		    retained->second.manifests != entry.second.manifests) {
			throw TransactionException("%s: catalog and metadata file disagree on retained snapshot %lld",
			                           REMOVE_ORPHAN_FILES_NAME, entry.first);
		}
	}
	for (const auto *source : {&file, &catalog}) {
		if (!source->refs) {
			continue;
		}
		for (const auto &ref : source->refs->additional_properties) {
			if (!file.snapshots.count(ref.second.snapshot_id)) {
				throw InvalidConfigurationException("%s: snapshot reference '%s' points to unknown snapshot %lld",
				                                    REMOVE_ORPHAN_FILES_NAME, ref.first, ref.second.snapshot_id);
			}
		}
	}
}

const char *ReferencedFileCollector::CollectFromMetadataFile(const string &path,
                                                             const rest_api_objects::TableMetadata &catalog,
                                                             const IcebergTableMetadata &embedded,
                                                             bool scan_manifests) {
	const bool added = references.Add(path, paths.ReferenceKey(path, true));
	ValidateOrphanPath(path);
	OpenFileInfo file(path);
	file_secret.Apply(file);
	const bool local = IsLocalOrphanPath(path);
	if (local && !fs.GetStatsIfExists(file)) {
		return MISSING_METADATA_LOCATION;
	}
	auto contents =
	    IcebergTableMetadata::Parse(file, fs, StringUtil::EndsWith(path, ".metadata.json.gz") ? "gzip" : "none");
	// Each LoadTable response must agree with its own metadata file.
	if (!CatalogMatchesMetadataFile(catalog, contents)) {
		throw TransactionException("%s: metadata file and catalog describe different table versions",
		                           REMOVE_ORPHAN_FILES_NAME);
	}
	auto metadata = IcebergTableMetadata::FromTableMetadata(contents);
	RequireUniqueSnapshotIds(contents, metadata);
	(void)metadata.GetLatestSnapshot();
	ValidateRetainedSnapshots(metadata, embedded);
	if (!GCEnabled(metadata)) {
		return GC_DISABLED;
	}
	if (added && local) {
		ValidateOrphanPath(path);
		references.AddLocalFile(fs, path);
	}
	CollectMetadataFiles(contents);
	if (scan_manifests) {
		ProtectMetadataDirectory(JoinOrphanPath(fs, table_location, "metadata"));
		if (contents.snapshots) {
			CollectSnapshots(metadata, *contents.snapshots);
		}
	}
	return nullptr;
}

bool DeleteListedFile(FileSystem &fs, const string &path) {
	try {
		if (IsLocalOrphanPath(path)) {
			ValidateOrphanPath(path);
		}
		// RemoveFile acts on the path. Concurrent writers must not replace it or adopt an old orphan.
		fs.RemoveFile(path);
		return true;
	} catch (const Exception &e) {
		const auto type = ErrorData(e).Type();
		if (type == ExceptionType::IO || type == ExceptionType::HTTP || type == ExceptionType::PERMISSION) {
			return false;
		}
		throw;
	}
}

// Check complete namespaces: a catalog may expose a leaf without exposing its parents.
void FindOrphanNamespace(ClientContext &context, IcebergCatalog &catalog, const string &name, idx_t start,
                         vector<string> &prefix, vector<string> &match) {
	for (auto dot = name.find('.', start + 1); dot < name.size() - 1; dot = name.find('.', dot + 1)) {
		prefix.push_back(name.substr(start, dot - start));
		FindOrphanNamespace(context, catalog, name, dot + 1, prefix, match);
		prefix.pop_back();
	}
	prefix.push_back(name.substr(start));
	context.InterruptCheck();
	if (IRCAPI::VerifySchemaExistence(context, catalog, prefix)) {
		if (!match.empty()) {
			throw InvalidInputException("%s: ambiguous Iceberg namespace '%s'", REMOVE_ORPHAN_FILES_NAME, name);
		}
		match = prefix;
	}
	prefix.pop_back();
}

unique_ptr<IcebergSchemaEntry> ResolveDottedOrphanSchema(ClientContext &context, IcebergCatalog &catalog,
                                                         IcebergTransaction &transaction,
                                                         const QualifiedName &table_name) {
	const auto &name = table_name.Schema().GetIdentifierName();
	const auto dot_count = std::count(name.begin(), name.end(), '.');
	if (dot_count == 0) {
		return nullptr;
	}
	if (transaction.deleted_schemas.count(name) || transaction.created_schemas.Contains(name)) {
		throw TransactionException("%s requires an unmodified catalog namespace '%s'", REMOVE_ORPHAN_FILES_NAME, name);
	}
	// The cache flattens ["a.b"] and ["a", "b"] to "a.b". Each dot can double the number of REST lookups.
	static constexpr int MAX_NAMESPACE_DOTS = 6;
	if (dot_count > MAX_NAMESPACE_DOTS) {
		throw InvalidInputException("%s: cannot verify Iceberg namespaces with more than %d dots",
		                            REMOVE_ORPHAN_FILES_NAME, MAX_NAMESPACE_DOTS);
	}
	vector<string> match;
	vector<string> prefix;
	FindOrphanNamespace(context, catalog, name, 0, prefix, match);
	if (match.empty()) {
		throw InvalidInputException("%s: schema '%s' not found in catalog '%s'", REMOVE_ORPHAN_FILES_NAME, name,
		                            table_name.Catalog().GetIdentifierName());
	}
	CreateSchemaInfo info;
	info.SetQualifiedName(table_name.WithName(Identifier()));
	auto schema = make_uniq<IcebergSchemaEntry>(catalog, info);
	schema->namespace_items = std::move(match);
	return schema;
}

RemoveOrphanFilesResult ExecuteRemoveOrphanFiles(ClientContext &context, const RemoveOrphanFilesPlanInput &input) {
	if (!input.dry_run && !context.transaction.IsAutoCommit()) {
		throw TransactionException("%s: physical file deletion requires autocommit", REMOVE_ORPHAN_FILES_NAME);
	}
	const auto default_cutoff = timestamp_t(Timestamp::GetCurrentTimestamp().value - DEFAULT_RETENTION_MICROS);
	const auto older_than = input.older_than.value_or(default_cutoff);
	if (!input.dry_run && older_than > default_cutoff) {
		return RemoveOrphanFilesResult("RETENTION_TOO_SHORT");
	}

	RemoveOrphanFilesResult result;
	auto &catalog = GetMaintenanceIcebergCatalog(context, input.table_name, REMOVE_ORPHAN_FILES_NAME);
	const auto table_name = catalog.FoldName(input.table_name);
	auto &transaction = IcebergTransaction::Get(context, catalog);
	// Serialize cleanup with other maintenance operators of this statement.
	lock_guard<mutex> table_update_guard(transaction.table_update_lock);
	auto dotted_schema = ResolveDottedOrphanSchema(context, catalog, transaction, table_name);
	auto &schema =
	    dotted_schema ? *dotted_schema : catalog.GetSchema(context, table_name.Schema()).Cast<IcebergSchemaEntry>();
	const auto &name = table_name.Name().GetIdentifierName();
	auto state = transaction.GetLatestTableState(IcebergTable::GetTableKey(catalog, schema.namespace_items, name));
	if (state && !state->IsUnmodifiedCatalogTable()) {
		throw TransactionException("%s requires an unmodified catalog table; use a new transaction for '%s'",
		                           REMOVE_ORPHAN_FILES_NAME, name);
	}
	auto load_table = [&](optional_ptr<const IcebergTableMetadata> expected) {
		auto load_result = IRCAPI::GetTable(context, catalog, schema, name);
		if (load_result.error_) {
			throw HTTPException(
			    {{"status_code", std::to_string(static_cast<int>(load_result.status_))},
			     {"iceberg_load_table_error_type", load_result.error_->_error.type}},
			    StringUtil::Format("GetTableInformation endpoint returned response code %s with message \"%s\"",
			                       EnumUtil::ToString(load_result.status_), load_result.error_->_error.message));
		}
		const auto &metadata = load_result.result_->result.metadata;
		if (expected && (metadata.table_uuid.empty() || metadata.table_uuid != expected->table_uuid ||
		                 metadata.location != expected->GetLocation())) {
			throw TransactionException("%s: table identity or location changed during orphan scanning",
			                           REMOVE_ORPHAN_FILES_NAME);
		}
		return load_result.result_;
	};
	auto response = load_table(state ? &state->GetInfo().GetMetadata() : nullptr);
	IcebergTable table_info(catalog, schema, name, response);
	const auto &catalog_metadata = table_info.GetMetadata();
	RequireUniqueSnapshotIds(response->result.metadata, catalog_metadata);
	if (!GCEnabled(catalog_metadata)) {
		return RemoveOrphanFilesResult(GC_DISABLED);
	}
	const auto &location = response->result.metadata_location;
	if (!location || location->empty()) {
		return RemoveOrphanFilesResult(MISSING_METADATA_LOCATION);
	}

	const auto &metadata_location = *location;
	auto &caller_fs = FileSystem::GetFileSystem(context);
	if (catalog_metadata.GetLocation().empty()) {
		return RemoveOrphanFilesResult(PREFIX_MISMATCH);
	}
	const auto table_location = ExpandStoredOrphanPath(caller_fs, catalog_metadata.GetLocation(), "table location");
	const auto scan_root = input.location.empty() ? table_location : caller_fs.ExpandPath(input.location);
	if (HasParentDirectoryReference(scan_root) || !HasOrphanPathPrefix(scan_root, table_location)) {
		return RemoveOrphanFilesResult(PREFIX_MISMATCH);
	}
	RequireAbsoluteOrphanPath(scan_root, "scan location");
	if (!input.dry_run && Path::FromString(scan_root).GetPathSegments().empty()) {
		throw InvalidConfigurationException("%s: physical deletion requires a directory below the storage root",
		                                    REMOVE_ORPHAN_FILES_NAME);
	}
	if (!input.dry_run && !IsLocalOrphanPath(scan_root) && !StringUtil::StartsWith(scan_root, "s3://")) {
		throw NotImplementedException("%s: physical deletion is only supported for local paths and s3://",
		                              REMOVE_ORPHAN_FILES_NAME);
	}
	const auto expanded_metadata_location = ExpandStoredOrphanPath(caller_fs, metadata_location, "metadata-location");

	auto secret_name = IcebergFileCredentialsCallback::Get(context, table_info);
	if (!secret_name.empty() && (!input.dry_run || !IsLocalOrphanPath(scan_root))) {
		throw InvalidConfigurationException(
		    "%s cannot verify the storage route for a table-level named secret because this operation resolves "
		    "secrets by path",
		    REMOVE_ORPHAN_FILES_NAME);
	}
	auto storage = OpenOrphanStorage(context, table_info, scan_root, expanded_metadata_location, !input.dry_run);
	auto &io = IsLocalOrphanPath(scan_root) ? context : *storage.context;
	auto &fs = FileSystem::GetFileSystem(io);
	OrphanPathNormalizer paths(io, scan_root, &context);
	OrphanFileReferences references(paths.ReferenceKey(scan_root));
	IcebergFileSecret file_secret(std::move(secret_name), std::to_string(table_info.catalog.GetOid()));
	ReferencedFileCollector collector(*storage.context, table_location, paths, file_secret, references);
	if (auto error = collector.CollectFromMetadataFile(expanded_metadata_location, response->result.metadata,
	                                                   catalog_metadata, true)) {
		return RemoveOrphanFilesResult(error);
	}
	collector.CollectMetadataFiles(response->result.metadata);
	auto scan_result = FindOrphanFiles({io, paths, references, scan_root, older_than, !input.dry_run});
	result.scanned_files = scan_result.scanned_count;
	result.referenced_files = static_cast<int64_t>(references.Count());
	if (scan_result.delete_candidates.empty()) {
		result.file_locations = std::move(scan_result.candidates);
		return result;
	}
	auto refreshed = load_table(catalog_metadata);
	const auto &head = refreshed->result;
	if (!head.metadata_location || head.metadata_location->empty()) {
		result.error_code = MISSING_METADATA_LOCATION;
		return result;
	}
	auto latest = IcebergTableMetadata::FromTableMetadata(head.metadata);
	RequireUniqueSnapshotIds(head.metadata, latest);
	if (!GCEnabled(latest)) {
		result.error_code = GC_DISABLED;
		return result;
	}
	// Appends publish new immutable files within the retention period. Only the first head
	// needs a manifest scan. Still protect metadata and statistics explicitly named by the new head.
	if (*head.metadata_location != metadata_location) {
		const auto path = ExpandStoredOrphanPath(FileSystem::GetFileSystem(*storage.context), *head.metadata_location,
		                                         "metadata-location");
		if (auto error = collector.CollectFromMetadataFile(path, head.metadata, latest, false)) {
			result.error_code = error;
			return result;
		}
	}
	collector.CollectMetadataFiles(head.metadata);
	using Candidate = decltype(scan_result.delete_candidates)::value_type;
	vector<const Candidate *> delete_order;
	delete_order.reserve(scan_result.delete_candidates.size());
	for (const auto &entry : scan_result.delete_candidates) {
		delete_order.push_back(&entry);
	}
	std::sort(delete_order.begin(), delete_order.end(),
	          [](const Candidate *a, const Candidate *b) { return a->first < b->first; });
	for (const auto *candidate : delete_order) {
		context.InterruptCheck();
		const auto &path = candidate->first;
		const auto &file = candidate->second;
		if (references.Contains(paths.ReferenceKey(path)) ||
		    (file.local_identity && references.ContainsLocalFile(*file.local_identity))) {
			continue;
		}
		if (file.observations == 1 && DeleteListedFile(fs, path)) {
			result.file_locations.push_back(path);
		} else {
			result.failed_file_locations.push_back(path);
			result.error_code = "FILE_DELETE_FAILED";
		}
	}
	return result;
}

struct RemoveOrphanFilesGlobalSourceState : public GlobalSourceState {
	bool emitted = false;
};

} // namespace

LogicalRemoveOrphanFiles::LogicalRemoveOrphanFiles(TableIndex bind_index_p, RemoveOrphanFilesPlanInput input_p)
    : LogicalExtensionOperator(), bind_index(bind_index_p), input(std::move(input_p)) {
	SetEstimatedCardinality(1);
}

void LogicalRemoveOrphanFiles::ResolveTypes() {
	types = RemoveOrphanFilesResultTypes();
}

vector<ColumnBinding> LogicalRemoveOrphanFiles::GetColumnBindings() {
	return GenerateColumnBindings(bind_index, 7);
}

vector<TableIndex> LogicalRemoveOrphanFiles::GetTableIndex() const {
	return {bind_index};
}

string LogicalRemoveOrphanFiles::GetName() const {
	return "ICEBERG_REMOVE_ORPHAN_FILES";
}

PhysicalOperator &LogicalRemoveOrphanFiles::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	return planner.Make<PhysicalRemoveOrphanFiles>(std::move(input), estimated_cardinality);
}

PhysicalRemoveOrphanFiles::PhysicalRemoveOrphanFiles(PhysicalPlan &physical_plan, RemoveOrphanFilesPlanInput input_p,
                                                     idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, RemoveOrphanFilesResultTypes(),
                       estimated_cardinality),
      input(std::move(input_p)) {
}

unique_ptr<GlobalSourceState> PhysicalRemoveOrphanFiles::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RemoveOrphanFilesGlobalSourceState>();
}

SourceResultType PhysicalRemoveOrphanFiles::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                            OperatorSourceInput &source_input) const {
	auto &state = source_input.global_state.Cast<RemoveOrphanFilesGlobalSourceState>();
	if (state.emitted) {
		return SourceResultType::FINISHED;
	}
	state.emitted = true;

	auto result = ExecuteRemoveOrphanFiles(context.client, input);
	WriteFileList(chunk.data[0], result.file_locations);
	chunk.data[1].Append(Value::BIGINT(input.dry_run ? 0 : NumericCast<int64_t>(result.file_locations.size())));
	chunk.data[2].Append(Value::BIGINT(result.scanned_files));
	chunk.data[3].Append(Value::BIGINT(result.referenced_files));
	chunk.data[4].Append(Value(result.error_code));
	WriteFileList(chunk.data[5], result.failed_file_locations);
	chunk.data[6].Append(Value::BIGINT(NumericCast<int64_t>(result.failed_file_locations.size())));
	chunk.SetChildCardinality(1);
	return SourceResultType::FINISHED;
}

string PhysicalRemoveOrphanFiles::GetName() const {
	return "ICEBERG_REMOVE_ORPHAN_FILES";
}

InsertionOrderPreservingMap<string> PhysicalRemoveOrphanFiles::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = input.table_name.ToString();
	result["Dry Run"] = input.dry_run ? "true" : "false";
	return result;
}

} // namespace duckdb
