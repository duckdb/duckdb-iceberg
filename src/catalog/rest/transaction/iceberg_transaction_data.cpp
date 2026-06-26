#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

namespace duckdb {

static constexpr const int64_t DEFAULT_RETRY_COUNT = 4;

static void LoadExistingManifestList(ClientContext &context, const IcebergTableMetadata &metadata,
                                     vector<IcebergManifestListEntry> &existing_manifest_list, int64_t &next_row_id) {
	existing_manifest_list.clear();

	auto current_snapshot = metadata.GetLatestSnapshot();
	if (!current_snapshot) {
		return;
	}

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = current_snapshot;
	snapshot_info.schema_id = metadata.GetCurrentSchemaId();

	auto &manifest_list_path = current_snapshot->manifest_list;
	auto scan =
	    AvroScan::ScanManifestList(snapshot_info, metadata, context, manifest_list_path, existing_manifest_list);
	auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);
	while (!manifest_list_reader->Finished()) {
		manifest_list_reader->Read();
	}

	if (metadata.iceberg_version < 3) {
		return;
	}

	//! Deal with upgraded tables, if the snapshot originated from V2
	for (auto &manifest_list_entry : existing_manifest_list) {
		auto &manifest_file = manifest_list_entry.file;
		if (manifest_file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (manifest_file.has_first_row_id) {
			continue;
		}
		if (current_snapshot->has_first_row_id) {
			throw InternalException("Table is corrupted, snapshot has 'first-row-id' but not all 'manifest_file' "
			                        "entries have a 'first_row_id'");
		}
		manifest_file.has_first_row_id = true;
		manifest_file.first_row_id = next_row_id;
		next_row_id += manifest_file.added_rows_count;
		next_row_id += manifest_file.existing_rows_count;
	}
}

IcebergTransactionData::IcebergTransactionData(ClientContext &context, const IcebergTableInformation &table_info)
    : commit_retry_count(DEFAULT_RETRY_COUNT), context(context), table_info(table_info) {
	initial_table_uuid = table_info.table_metadata.table_uuid;
	if (table_info.table_metadata.has_next_row_id) {
		next_row_id = table_info.table_metadata.next_row_id;
	}
	initial_schema_id = table_info.table_metadata.GetCurrentSchemaId();
	initial_default_spec_id = table_info.table_metadata.default_spec_id;
	if (table_info.table_metadata.HasSortOrder()) {
		initial_default_sort_order_id = table_info.table_metadata.default_sort_order_id;
	}

	auto it = table_info.table_metadata.table_properties.find("commit.retry.num-retries");
	if (it != table_info.table_metadata.table_properties.end()) {
		try {
			size_t processed = 0;
			commit_retry_count = std::stoll(it->second, &processed);
			if (processed != it->second.size()) {
				throw InvalidInputException(
				    "Invalid value '%s' for table property 'commit.retry.num-retries': expected an integer",
				    it->second);
			}
		} catch (std::exception &) {
			throw InvalidInputException(
			    "Invalid value '%s' for table property 'commit.retry.num-retries': expected an integer", it->second);
		}
		if (commit_retry_count < 0) {
			throw InvalidInputException(
			    "Invalid value '%s' for table property 'commit.retry.num-retries': expected a non-negative integer",
			    it->second);
		}
	}
}

int64_t IcebergTransactionData::GetCommitRetryCount() const {
	return commit_retry_count;
}

bool IcebergTransactionData::HasUpdates() const {
	if (!updates.empty()) {
		return true;
	}
	if (!requirements.empty()) {
		return true;
	}
	if (set_schema_id) {
		return true;
	}
	if (assert_schema_id) {
		return true;
	}
	return false;
}

bool IcebergTransactionData::SupportsAppendRetry() const {
	if (!requirements.empty() || set_schema_id) {
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

bool IcebergTransactionData::RetryStateMatches(const IcebergTableInformation &table) const {
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
	if (metadata.has_next_row_id) {
		loaded_next_row_id = metadata.next_row_id;
	}
	LoadExistingManifestList(context, metadata, existing_manifest_list, loaded_next_row_id);
	next_row_id = loaded_next_row_id;
}

void IcebergTransactionData::AddSnapshot(const IcebergTableMetadata &table_metadata,
                                         IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files,
                                         IcebergManifestDeletes &&altered_manifests) {
	//! NOTE: Lock has to be held to make sure the rows are assigned the correct row ids
	lock_guard<mutex> guard(lock);

	//! Generate a new snapshot id
	CacheExistingManifestList(guard, table_metadata);

	IcebergManifestContentType manifest_content_type;
	switch (operation) {
	case IcebergSnapshotOperationType::DELETE:
		manifest_content_type = IcebergManifestContentType::DELETE;
		break;
	case IcebergSnapshotOperationType::APPEND:
		manifest_content_type = IcebergManifestContentType::DATA;
		break;
	default:
		throw NotImplementedException("Cannot have use snapshot operation type REPLACE or OVERWRITE here");
	};

	auto bogus_snapshot_id = IcebergSnapshot::NewSnapshotId();
	auto temp_sequence_number = table_metadata.last_sequence_number + alters.size() + 1;

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_file =
	    IcebergManifestListEntry::CreateFromEntries(fs, bogus_snapshot_id, temp_sequence_number, table_metadata,
	                                                manifest_content_type, std::move(data_files), next_row_id);

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_metadata, operation);
	add_snapshot->AddManifestFile(std::move(manifest_file));
	// make sure we are still inserting into the current schema
	if (table_metadata.has_current_snapshot) {
		TableAddAssertCurrentSchemaId();
	}
	add_snapshot->altered_manifests = std::move(altered_manifests);

	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::AddUpdateSnapshot(const IcebergTableMetadata &table_metadata,
                                               vector<IcebergManifestEntry> &&delete_files,
                                               vector<IcebergManifestEntry> &&data_files,
                                               IcebergManifestDeletes &&altered_manifests) {
	//! NOTE: Lock has to be held to make sure the rows are assigned the correct row ids
	lock_guard<mutex> guard(lock);

	//! Generate a new snapshot id
	auto last_sequence_number = table_metadata.last_sequence_number;

	CacheExistingManifestList(guard, table_metadata);

	auto snapshot_id = IcebergSnapshot::NewSnapshotId();
	const auto sequence_number = last_sequence_number + alters.size() + 1;

	auto &fs = FileSystem::GetFileSystem(context);

	auto delete_manifest_file = IcebergManifestListEntry::CreateFromEntries(
	    fs, snapshot_id, sequence_number, table_metadata, IcebergManifestContentType::DELETE, std::move(delete_files),
	    next_row_id);
	// Add a manifest_file for the new insert data
	auto data_manifest_file = IcebergManifestListEntry::CreateFromEntries(
	    fs, snapshot_id, sequence_number, table_metadata, IcebergManifestContentType::DATA, std::move(data_files),
	    next_row_id);

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_metadata);
	add_snapshot->AddManifestFile(std::move(delete_manifest_file));
	add_snapshot->AddManifestFile(std::move(data_manifest_file));
	add_snapshot->altered_manifests = std::move(altered_manifests);

	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::TableAddSchema(const IcebergTableMetadata &table_metadata, int32_t schema_id) {
	auto add_schema_update = make_uniq<AddSchemaUpdate>(table_info, table_metadata, schema_id);
	updates.push_back(std::move(add_schema_update));
	updates.push_back(make_uniq<SetCurrentSchema>(table_metadata.GetCurrentSchemaId()));
	assert_schema_id = true;
	set_schema_id = true;
}

void IcebergTransactionData::TableSetCurrentSchema(const IcebergTableMetadata &table_metadata) {
	updates.push_back(make_uniq<SetCurrentSchema>(table_metadata.GetCurrentSchemaId()));
	set_schema_id = true;
}

void IcebergTransactionData::TableAssignUUID(const IcebergTableMetadata &table_metadata) {
	updates.push_back(make_uniq<AssignUUIDUpdate>(table_metadata.table_uuid));
}

void IcebergTransactionData::TableAddAssertCreate() {
	has_assert_create = true;
	requirements.push_back(make_uniq<AssertCreateRequirement>(table_info));
}

void IcebergTransactionData::TableAddAssertUUID() {
	requirements.push_back(make_uniq<AssertTableUUIDRequirement>(table_info));
}

void IcebergTransactionData::TableAddAssertCurrentSchemaId() {
	assert_schema_id = true;
}

void IcebergTransactionData::TableAddAssertLastAssignedFieldId() {
	requirements.push_back(make_uniq<AssertLastAssignedFieldIdRequirement>(table_info));
}

void IcebergTransactionData::TableAddAssertLastAssignedPartitionId() {
	requirements.push_back(make_uniq<AssertLastAssignedPartitionIdRequirement>(table_info));
}

void IcebergTransactionData::TableAddAssertDefaultSpecId() {
	requirements.push_back(make_uniq<AssertDefaultSpecIdRequirement>(table_info));
}

void IcebergTransactionData::TableAddUpradeFormatVersion(const IcebergTableMetadata &table_metadata) {
	updates.push_back(make_uniq<UpgradeFormatVersion>(table_metadata.iceberg_version));
}

void IcebergTransactionData::TableAddPartitionSpec(const IcebergTableMetadata &table_metadata) {
	updates.push_back(make_uniq<AddPartitionSpec>(table_metadata.GetLatestPartitionSpec()));
}

void IcebergTransactionData::TableAddSortOrder(const IcebergTableMetadata &table_metadata) {
	if (table_metadata.HasSortOrder()) {
		updates.push_back(make_uniq<AddSortOrder>(table_metadata.GetLatestSortOrder()));
		return;
	}
	IcebergSortOrder empty_sort_order;
	empty_sort_order.sort_order_id = AddSortOrder::DEFAULT_SORT_ORDER_ID;
	updates.push_back(make_uniq<AddSortOrder>(empty_sort_order));
}

void IcebergTransactionData::TableSetDefaultSortOrder(const IcebergTableMetadata &table_metadata) {
	if (table_metadata.HasSortOrder()) {
		updates.push_back(make_uniq<SetDefaultSortOrder>(table_metadata.GetLatestSortOrder().sort_order_id));
		return;
	}
	updates.push_back(make_uniq<SetDefaultSortOrder>(AddSortOrder::DEFAULT_SORT_ORDER_ID));
}

void IcebergTransactionData::TableSetDefaultSpec(const IcebergTableMetadata &table_metadata) {
	updates.push_back(make_uniq<SetDefaultSpec>(table_metadata.default_spec_id));
}

void IcebergTransactionData::TableSetProperties(const case_insensitive_map_t<string> &properties) {
	updates.push_back(make_uniq<SetProperties>(table_info, properties));
}

void IcebergTransactionData::TableRemoveProperties(const vector<string> &properties) {
	updates.push_back(make_uniq<RemoveProperties>(table_info, properties));
}

void IcebergTransactionData::TableSetLocation(const IcebergTableMetadata &table_metadata) {
	updates.push_back(make_uniq<SetLocation>(table_metadata.location));
}

} // namespace duckdb
