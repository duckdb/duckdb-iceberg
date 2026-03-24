#include "planning/metadata_io/avro/avro_scan.hpp"

#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"

#include "iceberg_extension.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/metadata_io/avro/iceberg_avro_multi_file_reader.hpp"
#include "planning/metadata_io/avro/iceberg_avro_multi_file_list.hpp"

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

	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	global_state = avro_scan->init_global(context, input);
}

unique_ptr<AvroScan> AvroScan::ScanManifest(const IcebergSnapshot &snapshot,
                                            vector<IcebergManifestListEntry> &manifest_files,
                                            const IcebergOptions &options, FileSystem &fs, const string &iceberg_path,
                                            const IcebergTableMetadata &metadata, ClientContext &context,
                                            optional_ptr<ManifestEntryReadState> read_state) {
	D_ASSERT(!manifest_files.empty());
	auto avro_scan_info = make_shared_ptr<IcebergManifestFileScanInfo>(metadata, snapshot, manifest_files, options, fs,
	                                                                   iceberg_path, read_state);
	return make_uniq<AvroScan>("placeholder", context, std::move(avro_scan_info));
}

unique_ptr<AvroScan> AvroScan::ScanManifestList(const IcebergSnapshot &snapshot, const IcebergTableMetadata &metadata,
                                                ClientContext &context, const string &path,
                                                vector<IcebergManifestListEntry> &result) {
	auto avro_scan_info = make_shared_ptr<IcebergManifestListScanInfo>(metadata, snapshot, result);
	return make_uniq<AvroScan>(path, context, std::move(avro_scan_info));
}

void AvroScan::InitializeChunk(DataChunk &chunk) const {
	chunk.Initialize(context, return_types, STANDARD_VECTOR_SIZE);
}

bool AvroScan::Finished() const {
	return finished;
}

const vector<column_t> &AvroScan::GetColumnIds() const {
	return column_ids;
}

const idx_t AvroScan::IcebergVersion() const {
	return scan_info->IcebergVersion();
}

} // namespace duckdb
