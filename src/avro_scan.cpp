#include "manifest_reader.hpp"
#include "avro_scan.hpp"
#include "iceberg_extension.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"

#include "iceberg_multi_file_reader.hpp"
#include "iceberg_avro_multi_file_reader.hpp"

namespace duckdb {

AvroScan::AvroScan(const string &scan_name, ClientContext &context, const string &path) : path(path), context(context) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	ExtensionHelper::AutoLoadExtension(instance, "avro");

	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "read_avro");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"read_avro\" not found!");
	}
	auto &avro_scan_entry = catalog_entry->Cast<TableFunctionCatalogEntry>();
	avro_scan = avro_scan_entry.functions.functions[0];
	avro_scan->get_multi_file_reader = IcebergAvroMultiFileReader::CreateInstance;

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = scan_name;
	dummy_table_function.get_multi_file_reader = IcebergAvroMultiFileReader::CreateInstance;
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	bind_data = avro_scan->bind(context, bind_input, return_types, return_names);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	global_state = avro_scan->init_global(context, input);
	local_state = avro_scan->init_local(execution_context, input, global_state.get());
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
