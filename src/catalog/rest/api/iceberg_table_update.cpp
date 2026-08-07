#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/api/iceberg_manifest_merge.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

namespace duckdb {

static void AssignManifestFirstRowIds(const IcebergTableMetadata &metadata,
                                      optional_ptr<const IcebergSnapshot> current_snapshot,
                                      vector<IcebergManifestListEntry> &existing_manifest_list, int64_t &next_row_id) {
	if (metadata.iceberg_version < 3) {
		return;
	}
	for (auto &manifest_list_entry : existing_manifest_list) {
		auto &manifest_file = manifest_list_entry.file;
		if (manifest_file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (manifest_file.first_row_id) {
			D_ASSERT(manifest_file.counts && manifest_file.counts->added_rows_count &&
			         manifest_file.counts->existing_rows_count);
			next_row_id =
			    MaxValue<int64_t>(next_row_id, *manifest_file.first_row_id + *manifest_file.counts->added_rows_count +
			                                       *manifest_file.counts->existing_rows_count);
			continue;
		}
		if (current_snapshot && current_snapshot->first_row_id) {
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
}

IcebergCommitState::IcebergCommitState(const IcebergTable &table_info, ClientContext &context)
    : table_info(table_info), context(context) {
	RefreshFromTable();
}

void IcebergCommitState::RefreshFromTable() {
	next_sequence_number = table_info.table_metadata.last_sequence_number + 1;
	next_row_id = 0;
	if (table_info.table_metadata.next_row_id) {
		next_row_id = *table_info.table_metadata.next_row_id;
	}
}

void IcebergCommitState::LoadExistingManifests(DatabaseInstance &db,
                                               vector<IcebergManifestListEntry> &&existing_manifests) {
	manifests = std::move(existing_manifests);
	auto current_snapshot = table_info.table_metadata.GetLatestSnapshot();
	latest_snapshot = current_snapshot;
	if (manifests.empty() && current_snapshot) {
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = current_snapshot;
		snapshot_info.schema_id = table_info.table_metadata.GetCurrentSchemaId();

		IcebergManifestList::LoadManifestFiles(snapshot_info, table_info.table_metadata, context, manifests);
	}

	//! In V1 the added/deleted/existing file counts were optional
	//! But even if the attributes are present, they're allowed to be NULL
	//! In both those situations we have to read all the entries of the manifest to materialize these counts on-demand
	for (auto &manifest : manifests) {
		if (manifest.file.counts && manifest.file.counts->Complete()) {
			continue;
		}
		manifest =
		    IcebergManifestMerge::ScanManifestEntries(manifest, *this, table_info.table_metadata.GetCurrentSchemaId());
	}

	next_row_id = 0;
	if (table_info.table_metadata.next_row_id) {
		next_row_id = *table_info.table_metadata.next_row_id;
	}
	AssignManifestFirstRowIds(table_info.table_metadata, current_snapshot, manifests, next_row_id);

	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;
	IcebergManifestMerge::MergeManifestList(manifests, table_info.table_metadata.GetCurrentSchemaId(), avro_copy, db,
	                                        *this);
}

IcebergTableUpdate::IcebergTableUpdate(IcebergTableUpdateType type) : type(type) {
}

} // namespace duckdb
