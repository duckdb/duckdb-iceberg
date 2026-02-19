#include "manifest_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "avro_scan.hpp"

namespace duckdb {

BaseManifestReader::BaseManifestReader(const AvroScan &scan_p) : scan(scan_p), iceberg_version(scan.IcebergVersion()) {
}

BaseManifestReader::~BaseManifestReader() {
}

void BaseManifestReader::InitializeInternal() {
	ThreadContext thread_context(scan.context);
	ExecutionContext execution_context(scan.context, thread_context, nullptr);
	TableFunctionInitInput input(scan.bind_data.get(), scan.GetColumnIds(), vector<idx_t>(), nullptr);
	local_state = scan.avro_scan->init_local(execution_context, input, scan.global_state.get());

	scan.InitializeChunk(chunk);

	auto &multi_file_bind_data = scan.bind_data->Cast<MultiFileBindData>();
	auto &columns = multi_file_bind_data.reader_bind.schema;
	initialized = true;
}

const IcebergAvroScanInfo &BaseManifestReader::GetScanInfo() const {
	return *scan.scan_info;
}

idx_t BaseManifestReader::ScanInternal(idx_t remaining) {
	if (!initialized) {
		InitializeInternal();
	}
	if (finished) {
		return 0;
	}

	if (offset >= chunk.size()) {
		TableFunctionInput function_input(scan.bind_data.get(), local_state.get(), scan.global_state.get());
		scan.avro_scan->function(scan.context, function_input, chunk);
		auto count = chunk.size();
		for (auto &vec : chunk.data) {
			vec.Flatten(count);
		}

		if (count == 0) {
			finished = true;
			return 0;
		}
		offset = 0;
	}
	return MinValue(chunk.size() - offset, remaining);
}

bool BaseManifestReader::Finished() const {
	return initialized && finished;
}

} // namespace duckdb
