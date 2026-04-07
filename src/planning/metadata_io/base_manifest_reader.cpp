#include "planning/metadata_io/base_manifest_reader.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"

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
	initialized = true;
}

const IcebergAvroScanInfo &BaseManifestReader::GetScanInfo() const {
	return *scan.scan_info;
}

void BaseManifestReader::ScanInternal() {
	if (!initialized) {
		InitializeInternal();
	}
	if (finished) {
		return;
	}
	TableFunctionInput function_input(scan.bind_data.get(), local_state.get(), scan.global_state.get());
	scan.avro_scan->function(scan.context, function_input, chunk);
	if (chunk.size() == 0) {
		finished = true;
	}
}

bool BaseManifestReader::Finished() const {
	return initialized && finished;
}

} // namespace duckdb
