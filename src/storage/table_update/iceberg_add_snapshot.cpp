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

static rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergTableInformation &table_info,
                                                             const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.has_add_snapshot_update = true;
	auto &update = table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject(table_info);
	return table_update;
}

static IcebergManifestFile WriteManifestListEntry(IcebergTableInformation &table_info,
                                                  const IcebergManifestFile &manifest_file, CopyFunction &avro_copy,
                                                  DatabaseInstance &db, ClientContext &context) {
	auto manifest_length =
	    manifest_file::WriteToFile(table_info.table_metadata, manifest_file.manifest_file, avro_copy, db, context);
	IcebergManifestFile new_manifest_file(manifest_file);
	new_manifest_file.manifest_length = manifest_length;
	return new_manifest_file;
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                      IcebergCommitState &commit_state) const {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	auto &uncommitted_manifest_files = manifest_list.GetManifestFilesConst();
	D_ASSERT(!uncommitted_manifest_files.empty());

	IcebergManifestList new_manifest_list(manifest_list.GetPath());
	for (auto &manifest_file : uncommitted_manifest_files) {
		new_manifest_list.AddManifestFile(WriteManifestListEntry(table_info, manifest_file, avro_copy, db, context));
	}
	new_manifest_list.AddToManifestEntries(commit_state.manifests);

	manifest_list::WriteToFile(table_info.table_metadata, new_manifest_list, avro_copy, db, context);
	commit_state.manifests = new_manifest_list.GetManifestListEntries();

	commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate(table_info, snapshot));
}

} // namespace duckdb
