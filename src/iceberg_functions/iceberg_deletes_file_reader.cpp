#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "iceberg_functions/iceberg_deletes_file_reader.hpp"
#include "iceberg_functions.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"

namespace duckdb {

virtual_column_map_t IcebergDeleteVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto result = IcebergTableEntry::VirtualColumns();
	bind_data.virtual_columns = result;
	return result;
}

static void IcebergDeletesScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                        const TableFunction &function) {
	throw NotImplementedException("IcebergDeletesScan serialization not implemented");
}
static unique_ptr<FunctionData> IcebergDeletesScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("IcebergDeletesScan deserialization not implemented");
}

TableFunctionSet IcebergFunctions::GetIcebergDeletesScanFunction(ClientContext &context) {
	// The iceberg_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
	// IcebergMultiFileReader into it to create a Iceberg-based multi file read
	auto &instance = DatabaseInstance::GetDatabase(context);
	//! FIXME: delete files could also be made without row_ids,
	//! in which case we need to rely on the `'schema.column-mapping.default'` property just like data files do.
	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "parquet_scan");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"parquet_scan\" not found!");
	}
	auto &parquet_scan = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto parquet_scan_copy = parquet_scan.functions;

	for (auto &function : parquet_scan_copy.functions) {
		// Register the MultiFileReader as the driver for reads
		function.get_multi_file_reader = IcebergDeleteFileReader::CreateInstance;
		function.late_materialization = false;

		// Unset all of these: they are either broken, very inefficient.
		// TODO: implement/fix these
		function.serialize = IcebergDeletesScanSerialize;
		function.deserialize = IcebergDeletesScanDeserialize;

		function.statistics = nullptr;
		function.table_scan_progress = nullptr;
		function.get_bind_info = nullptr;
		function.get_virtual_columns = IcebergDeleteVirtualColumns;

		// Schema param is just confusing here
		function.named_parameters.erase("schema");
		function.name = "iceberg_deletes_scan";
	}

	parquet_scan_copy.name = "iceberg_deletes_scan";
	return parquet_scan_copy;
}

IcebergDeleteFileReader::IcebergDeleteFileReader(shared_ptr<TableFunctionInfo> function_info)
    : function_info(function_info) {
}

unique_ptr<MultiFileReader> IcebergDeleteFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<IcebergDeleteFileReader>(table.function_info);
}

shared_ptr<MultiFileList> IcebergDeleteFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                  const FileGlobInput &glob_input) {
	D_ASSERT(paths.size() == 1);
	vector<OpenFileInfo> open_files;
	// in case someone calls this
	if (!function_info) {
		throw NotImplementedException("IcebergDeleteFileReader must be called with function info");
	}
	auto &iceberg_delete_function_info = function_info->Cast<IcebergDeleteScanInfo>();
	auto &extended_delete_info = iceberg_delete_function_info.file_info;
	open_files.emplace_back(extended_delete_info);
	auto res = make_uniq<SimpleMultiFileList>(std::move(open_files));
	return std::move(res);
}

} // namespace duckdb
