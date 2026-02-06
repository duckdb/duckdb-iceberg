#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "storage/catalog/iceberg_table_set.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(IcebergTableInformation &table_info, const string &manifest_list_path,
                                       IcebergSnapshot &&snapshot)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT, table_info), manifest_list(manifest_list_path),
      snapshot(std::move(snapshot)) {
}

rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.has_add_snapshot_update = true;
	auto &update = table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject();
	return table_update;
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	D_ASSERT(manifest_list.GetManifestListEntriesCount() != 0);
	auto manifest_list_entries_size = manifest_list.GetManifestListEntriesCount();
	for (idx_t manifest_index = 0; manifest_index < manifest_list_entries_size; manifest_index++) {
		manifest_list.WriteManifestListEntry(table_info, manifest_index, avro_copy, db, context);
	}

	// Add manifest files from previous snapshots
	manifest_list.AddToManifestEntries(commit_state.manifests);
	manifest_list::WriteToFile(manifest_list, avro_copy, db, context);
	commit_state.manifests = manifest_list.GetManifestListEntries();

	commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate(snapshot));
}

} // namespace duckdb
