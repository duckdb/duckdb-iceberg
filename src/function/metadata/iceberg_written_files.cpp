#include "catalog/rest/transaction/iceberg_written_files.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "function/iceberg_functions.hpp"

namespace duckdb {

namespace {

struct IcebergWrittenFilesGlobalState : public GlobalTableFunctionState {
	vector<IcebergWrittenFile> files;
	idx_t offset = 0;
};

unique_ptr<FunctionData> IcebergWrittenFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("file_path");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("record_count");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("file_size_in_bytes");
	return_types.push_back(LogicalType::BIGINT);
	return make_uniq<TableFunctionData>();
}

unique_ptr<GlobalTableFunctionState> IcebergWrittenFilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<IcebergWrittenFilesGlobalState>();
	// read when the query runs, so a prepared statement sees the latest write
	result->files = IcebergWrittenFilesState::Get(context)->GetFiles();
	return std::move(result);
}

void IcebergWrittenFilesScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<IcebergWrittenFilesGlobalState>();
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.files.size() - state.offset);
	auto paths = FlatVector::Writer<string_t>(output.data[0], count);
	auto record_counts = FlatVector::Writer<int64_t>(output.data[1], count);
	auto sizes = FlatVector::Writer<int64_t>(output.data[2], count);
	for (idx_t i = 0; i < count; i++) {
		auto &file = state.files[state.offset + i];
		paths.WriteValue(string_t(file.file_path));
		record_counts.WriteValue(file.record_count);
		sizes.WriteValue(file.file_size_in_bytes);
	}
	state.offset += count;
	output.SetCardinality(count);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergWrittenFilesFunction() {
	TableFunctionSet function_set("iceberg_written_files");
	function_set.AddFunction(
	    TableFunction({}, IcebergWrittenFilesScan, IcebergWrittenFilesBind, IcebergWrittenFilesInit));
	return function_set;
}

} // namespace duckdb
