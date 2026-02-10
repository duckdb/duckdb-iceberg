#include "manifest_reader.hpp"
#include "avro_scan.hpp"
#include "iceberg_extension.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"

#include "iceberg_multi_file_reader.hpp"
#include "iceberg_avro_multi_file_reader.hpp"
#include "iceberg_avro_multi_file_list.hpp"

namespace duckdb {

namespace {

struct ManifestFileVirtualColumn {
	column_t id;
	const char *name;
	LogicalType type;
};

} // namespace

AvroScan::AvroScan(const string &path, ClientContext &context, shared_ptr<IcebergAvroScanInfo> avro_scan_info)
    : context(context), scan_info(avro_scan_info) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "read_avro");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"read_avro\" not found!");
	}
	auto &avro_scan_entry = catalog_entry->Cast<TableFunctionCatalogEntry>();
	avro_scan = avro_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	const bool is_manifest_list = avro_scan_info->type == AvroScanInfoType::MANIFEST_LIST;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = is_manifest_list ? "IcebergManifestList" : "IcebergManifest";
	dummy_table_function.get_multi_file_reader = IcebergAvroMultiFileReader::CreateInstance;
	dummy_table_function.function_info = avro_scan_info;

	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	bind_data = avro_scan->bind(context, bind_input, return_types, return_names);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}

	if (!is_manifest_list) {
		const ManifestFileVirtualColumn columns[] = {
		    {IcebergAvroMultiFileReader::PARTITION_SPEC_ID_FIELD_ID, "partition_spec_id", LogicalType::INTEGER},
		    {IcebergAvroMultiFileReader::SEQUENCE_NUMBER_FIELD_ID, "sequence_number", LogicalType::BIGINT},
		    {IcebergAvroMultiFileReader::MANIFEST_FILE_INDEX_FIELD_ID, "manifest_file_index", LogicalType::UBIGINT}};
		const idx_t columns_size = sizeof(columns) / sizeof(ManifestFileVirtualColumn);
		auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();

		virtual_column_map_t result;
		for (idx_t i = 0; i < columns_size; i++) {
			auto &column = columns[i];
			result.emplace(column.id, TableColumn(column.name, column.type));
			column_ids.push_back(column.id);
			return_types.push_back(column.type);
			return_names.push_back(column.name);
		}
		multi_file_bind_data.virtual_columns = result;
	}

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	global_state = avro_scan->init_global(context, input);
	local_state = avro_scan->init_local(execution_context, input, global_state.get());
}

unique_ptr<AvroScan> AvroScan::ScanManifest(const IcebergSnapshot &snapshot,
                                            const vector<IcebergManifestFile> &manifest_files,
                                            const IcebergOptions &options, FileSystem &fs, const string &iceberg_path,
                                            const IcebergTableMetadata &metadata, ClientContext &context) {
	D_ASSERT(!manifest_files.empty());
	auto avro_scan_info =
	    make_shared_ptr<IcebergManifestFileScanInfo>(metadata, snapshot, manifest_files, options, fs, iceberg_path);
	return make_uniq<AvroScan>("placeholder", context, std::move(avro_scan_info));
}

unique_ptr<AvroScan> AvroScan::ScanManifestList(const IcebergSnapshot &snapshot, const IcebergTableMetadata &metadata,
                                                ClientContext &context, const string &path) {
	auto avro_scan_info = make_shared_ptr<IcebergManifestListScanInfo>(metadata, snapshot);
	return make_uniq<AvroScan>(path, context, std::move(avro_scan_info));
}

bool AvroScan::GetNext(DataChunk &result) {
	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	avro_scan->function(context, function_input, result);

	idx_t count = result.size();
	for (auto &vec : result.data) {
		vec.Flatten(count);
	}
	if (count == 0) {
		finished = true;
		return false;
	}
	return true;
}

void AvroScan::InitializeChunk(DataChunk &chunk) {
	chunk.Initialize(context, return_types, STANDARD_VECTOR_SIZE);
}

bool AvroScan::Finished() const {
	return finished;
}

} // namespace duckdb
