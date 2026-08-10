#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

#include "catalog/rest/transaction/iceberg_transaction.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

namespace duckdb {

static void LoadMissingManifestCounts(ClientContext &context, const IcebergTableMetadata &metadata,
                                      const IcebergSnapshotScanInfo &snapshot_info,
                                      IcebergManifestListEntry &manifest_list_entry) {
	if (manifest_list_entry.file.counts && manifest_list_entry.file.counts->Complete()) {
		return;
	}
	vector<IcebergManifestListEntry> manifest_files;
	manifest_files.push_back(manifest_list_entry);
	manifest_files[0].manifest_entries.reset();

	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(context);
	auto scan = AvroScan::ScanManifest(snapshot_info, manifest_files, options, fs, "", metadata, context);
	auto reader = make_uniq<manifest_file::ManifestReader>(*scan);
	while (!reader->Finished()) {
		reader->Read();
	}

	manifest_list_entry = std::move(manifest_files[0]);
	manifest_list_entry.file.SetCountsFromEntries(manifest_list_entry.GetManifestEntries());
}

static optional<int64_t> LoadExistingManifestList(ClientContext &context, const IcebergTableMetadata &metadata,
                                                  vector<IcebergManifestListEntry> &existing_manifest_list,
                                                  int64_t &next_row_id) {
	existing_manifest_list.clear();

	auto current_snapshot = metadata.GetLatestSnapshot();
	if (!current_snapshot) {
		return {};
	}
	optional<int64_t> base_snapshot_id = current_snapshot->snapshot_id;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = current_snapshot;
	snapshot_info.schema_id = metadata.GetCurrentSchemaId();

	IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context, existing_manifest_list);
	for (auto &manifest_list_entry : existing_manifest_list) {
		LoadMissingManifestCounts(context, metadata, snapshot_info, manifest_list_entry);
	}

	if (metadata.iceberg_version < 3) {
		return base_snapshot_id;
	}

	//! Deal with upgraded tables, if the snapshot originated from V2
	for (auto &manifest_list_entry : existing_manifest_list) {
		auto &manifest_file = manifest_list_entry.file;
		if (manifest_file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (manifest_file.first_row_id) {
			continue;
		}
		if (current_snapshot->first_row_id) {
			throw InvalidConfigurationException(
			    "Table is corrupted, snapshot has 'first-row-id' but not all 'manifest_file' "
			    "entries have a 'first_row_id'");
		}
		D_ASSERT(manifest_file.counts && manifest_file.counts->added_rows_count &&
		         manifest_file.counts->existing_rows_count);
		manifest_file.first_row_id = next_row_id;
		next_row_id += *manifest_file.counts->added_rows_count;
		next_row_id += *manifest_file.counts->existing_rows_count;
	}
	return base_snapshot_id;
}

IcebergTransactionData::IcebergTransactionData(ClientContext &context, IcebergTransaction &transaction,
                                               const IcebergTable &table_info)
    : context(context), transaction(transaction), table_info(table_info) {
	initial_table_uuid = table_info.table_metadata.table_uuid;
	if (table_info.table_metadata.next_row_id) {
		next_row_id = *table_info.table_metadata.next_row_id;
	}
	initial_schema_id = table_info.table_metadata.GetCurrentSchemaId();
	initial_default_spec_id = table_info.table_metadata.default_spec_id;
	if (table_info.table_metadata.HasSortOrder()) {
		initial_default_sort_order_id = table_info.table_metadata.default_sort_order_id;
	}
}

int64_t IcebergTransactionData::GetCommitRetryCount() const {
	static constexpr const int64_t DEFAULT_RETRY_COUNT = 4;
	auto it = table_info.table_metadata.table_properties.find("commit.retry.num-retries");
	if (it == table_info.table_metadata.table_properties.end()) {
		return DEFAULT_RETRY_COUNT;
	}
	int64_t result;
	try {
		size_t processed = 0;
		result = std::stoll(it->second, &processed);
		if (processed != it->second.size()) {
			throw InvalidInputException(
			    "Invalid value '%s' for table property 'commit.retry.num-retries': expected an integer", it->second);
		}
	} catch (std::exception &) {
		throw InvalidInputException(
		    "Invalid value '%s' for table property 'commit.retry.num-retries': expected an integer", it->second);
	}
	if (result < 0) {
		throw InvalidInputException(
		    "Invalid value '%s' for table property 'commit.retry.num-retries': expected a non-negative integer",
		    it->second);
	}
	return result;
}

bool IcebergTransactionData::ContainsDelete() const {
	for (auto &update : updates) {
		if (update->type != IcebergTableUpdateType::ADD_SNAPSHOT) {
			continue;
		}
		if (update->Cast<IcebergAddSnapshot>().GetOperation() == IcebergSnapshotOperationType::DELETE) {
			return true;
		}
	}
	return false;
}

bool IcebergTransactionData::IsFileInvalidated(const string &file_path) const {
	return manifest_deletes.IsInvalidated(file_path);
}

bool IcebergTransactionData::SupportsAppendRetry() const {
	if (!requirements.empty() || pending_current_schema_id.has_value()) {
		return false;
	}
	if (updates.empty()) {
		return false;
	}
	for (auto &update : updates) {
		if (!update->IsRetryable()) {
			return false;
		}
	}
	return true;
}

bool IcebergTransactionData::RetryStateMatches(const IcebergTable &table) const {
	if (table.table_metadata.table_uuid != initial_table_uuid) {
		return false;
	}
	if (table.table_metadata.GetCurrentSchemaId() != initial_schema_id) {
		return false;
	}
	if (table.table_metadata.default_spec_id != initial_default_spec_id) {
		return false;
	}
	if (table.table_metadata.HasSortOrder() != initial_default_sort_order_id.IsValid()) {
		return false;
	}
	if (table.table_metadata.HasSortOrder() &&
	    table.table_metadata.default_sort_order_id.GetIndex() != initial_default_sort_order_id.GetIndex()) {
		return false;
	}
	return true;
}

void IcebergTransactionData::CacheExistingManifestList(lock_guard<mutex> &guard, const IcebergTableMetadata &metadata) {
	if (!alters.empty()) {
		return;
	}
	int64_t loaded_next_row_id = 0;
	if (metadata.next_row_id) {
		loaded_next_row_id = *metadata.next_row_id;
	}
	base_snapshot_id = LoadExistingManifestList(context, metadata, existing_manifest_list, loaded_next_row_id);
	next_row_id = loaded_next_row_id;
}

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files,
                                         IcebergManifestDeletes &&altered_manifests) {
	//! NOTE: Lock has to be held to make sure the rows are assigned the correct row ids
	lock_guard<mutex> guard(lock);

	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	CacheExistingManifestList(guard, table_metadata);

	IcebergManifestContentType manifest_content_type;
	switch (operation) {
	case IcebergSnapshotOperationType::APPEND:
	case IcebergSnapshotOperationType::REPLACE:
		//! This helper currently writes DATA manifest entries; REPLACE itself is not limited to data files.
		manifest_content_type = IcebergManifestContentType::DATA;
		break;
	default:
		throw NotImplementedException("Snapshot operation type %d does not write data manifests",
		                              static_cast<uint8_t>(operation));
	};

	auto temp_sequence_number = table_metadata.last_sequence_number + alters.size() + 1;

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_metadata = IcebergManifestMetadata::FromTableMetadata(table_metadata, manifest_content_type);
	auto manifest_file = IcebergManifestListEntry::CreateFromEntries(
	    fs, temp_sequence_number, table_metadata, manifest_metadata, std::move(data_files), next_row_id);

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, operation);
	add_snapshot->AddManifestFile(std::move(manifest_file));
	// make sure we are still inserting into the current schema
	if (table_metadata.current_snapshot_id) {
		TableAddAssertCurrentSchemaId();
	}
	AddSnapshotUpdate(std::move(add_snapshot), std::move(altered_manifests));
}

void IcebergTransactionData::AddDeleteManifestFiles(IcebergAddSnapshot &add_snapshot,
                                                    partitioned_manifest_entry_map_t &&delete_files,
                                                    sequence_number_t sequence_number) {
	auto &table_metadata = table_info.table_metadata;
	auto &fs = FileSystem::GetFileSystem(context);
	//! One manifest per partition spec: a manifest declares a single spec, and the entries it holds carry
	//! partition values in that spec.
	for (auto &entry : delete_files) {
		auto manifest_metadata =
		    IcebergManifestMetadata::FromTableMetadata(table_metadata, IcebergManifestContentType::DELETE, entry.first);
		add_snapshot.AddManifestFile(IcebergManifestListEntry::CreateFromEntries(
		    fs, sequence_number, table_metadata, manifest_metadata, std::move(entry.second), next_row_id));
	}
}

void IcebergTransactionData::AddSnapshotUpdate(unique_ptr<IcebergAddSnapshot> add_snapshot,
                                               IcebergManifestDeletes &&altered_manifests) {
	auto versioned_deletes = manifest_deletes.AtVersion(alters.size());
	if (versioned_deletes.Merge(std::move(altered_manifests))) {
		add_snapshot->SetManifestDeletes(std::move(versioned_deletes));
	}
	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::AddDeleteSnapshot(partitioned_manifest_entry_map_t &&delete_files,
                                               IcebergManifestDeletes &&altered_manifests) {
	//! NOTE: Lock has to be held to make sure the rows are assigned the correct row ids
	lock_guard<mutex> guard(lock);

	auto &table_metadata = table_info.table_metadata;
	CacheExistingManifestList(guard, table_metadata);

	const auto sequence_number = table_metadata.last_sequence_number + alters.size() + 1;

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, IcebergSnapshotOperationType::DELETE);
	AddDeleteManifestFiles(*add_snapshot, std::move(delete_files), sequence_number);
	// make sure we are still inserting into the current schema
	if (table_metadata.current_snapshot_id) {
		TableAddAssertCurrentSchemaId();
	}
	AddSnapshotUpdate(std::move(add_snapshot), std::move(altered_manifests));
}

void IcebergTransactionData::AddUpdateSnapshot(partitioned_manifest_entry_map_t &&delete_files,
                                               vector<IcebergManifestEntry> &&data_files,
                                               IcebergManifestDeletes &&altered_manifests) {
	//! NOTE: Lock has to be held to make sure the rows are assigned the correct row ids
	lock_guard<mutex> guard(lock);

	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto last_sequence_number = table_metadata.last_sequence_number;

	CacheExistingManifestList(guard, table_metadata);

	const auto sequence_number = last_sequence_number + alters.size() + 1;

	auto &fs = FileSystem::GetFileSystem(context);
	auto data_manifest_metadata =
	    IcebergManifestMetadata::FromTableMetadata(table_metadata, IcebergManifestContentType::DATA);

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info);
	AddDeleteManifestFiles(*add_snapshot, std::move(delete_files), sequence_number);
	// Add a manifest_file for the new insert data
	add_snapshot->AddManifestFile(IcebergManifestListEntry::CreateFromEntries(
	    fs, sequence_number, table_metadata, data_manifest_metadata, std::move(data_files), next_row_id));
	AddSnapshotUpdate(std::move(add_snapshot), std::move(altered_manifests));
}

void IcebergTransactionData::TableAddSchema(int32_t schema_id) {
	auto schema = table_info.table_metadata.GetSchemaFromId(schema_id);
	if (!schema) {
		throw InternalException("(TableAddSchema) Couldn't find schema with id: %d", schema_id);
	}
	auto add_schema_update = make_uniq<AddSchemaUpdate>(schema->Copy(), table_info.table_metadata.last_column_id);
	updates.push_back(std::move(add_schema_update));
	assert_schema_id = true;
	pending_current_schema_id = schema_id;
}

void IcebergTransactionData::TableSetCurrentSchema(int32_t schema_id) {
	pending_current_schema_id = schema_id;
}

void IcebergTransactionData::TableAssignUUID() {
	updates.push_back(make_uniq<AssignUUIDUpdate>(table_info.table_metadata.table_uuid));
}

void IcebergTransactionData::TableAddAssertCreate() {
	has_assert_create = true;
	requirements.push_back(make_uniq<AssertCreateRequirement>());
	auto alter_update = transaction.GetAlterUpdate();
	D_ASSERT(alter_update);
	transaction.VerifyAlterUpdateAtomicity(*alter_update);
}

void IcebergTransactionData::TableAddAssertUUID() {
	requirements.push_back(make_uniq<AssertTableUUIDRequirement>(table_info.table_metadata.table_uuid));
}

void IcebergTransactionData::TableAddAssertCurrentSchemaId() {
	assert_schema_id = true;
}

void IcebergTransactionData::TableAddAssertLastAssignedFieldId() {
	D_ASSERT(table_info.table_metadata.HasLastColumnId());
	requirements.push_back(make_uniq<AssertLastAssignedFieldIdRequirement>(
	    static_cast<int32_t>(table_info.table_metadata.GetLastColumnId())));
}

void IcebergTransactionData::TableAddAssertLastAssignedPartitionId() {
	int32_t last_assigned_partition_id = 999;
	if (table_info.table_metadata.HasLastPartitionId()) {
		last_assigned_partition_id = table_info.table_metadata.GetLastPartitionFieldId();
	}
	requirements.push_back(make_uniq<AssertLastAssignedPartitionIdRequirement>(last_assigned_partition_id));
}

void IcebergTransactionData::TableAddAssertDefaultSpecId() {
	requirements.push_back(make_uniq<AssertDefaultSpecIdRequirement>(table_info.table_metadata.default_spec_id));
}

void IcebergTransactionData::TableAddUpradeFormatVersion() {
	updates.push_back(make_uniq<UpgradeFormatVersion>(table_info.table_metadata.iceberg_version));
}

void IcebergTransactionData::TableAddPartitionSpec() {
	updates.push_back(make_uniq<AddPartitionSpec>(table_info.table_metadata.GetLatestPartitionSpec()));
}

void IcebergTransactionData::TableAddSortOrder() {
	updates.push_back(make_uniq<AddSortOrder>(table_info.table_metadata.GetLatestSortOrder()));
}

void IcebergTransactionData::TableSetDefaultSortOrder() {
	D_ASSERT(table_info.table_metadata.HasSortOrder());
	updates.push_back(make_uniq<SetDefaultSortOrder>(table_info.table_metadata.GetLatestSortOrder().sort_order_id));
}

void IcebergTransactionData::TableSetDefaultSpec() {
	updates.push_back(make_uniq<SetDefaultSpec>(table_info.table_metadata.default_spec_id));
}

void IcebergTransactionData::TableSetProperties(const case_insensitive_map_t<string> &properties) {
	updates.push_back(make_uniq<SetProperties>(properties));
}

void IcebergTransactionData::TableRemoveProperties(const vector<string> &properties) {
	updates.push_back(make_uniq<RemoveProperties>(properties));
}

void IcebergTransactionData::TableSetLocation() {
	updates.push_back(make_uniq<SetLocation>(table_info.table_metadata.location));
}

} // namespace duckdb
