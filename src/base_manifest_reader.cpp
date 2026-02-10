#include "manifest_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"

namespace duckdb {

void BaseManifestReader::Initialize(unique_ptr<AvroScan> scan_p) {
	const bool first_init = scan == nullptr;
	scan = std::move(scan_p);
	if (!first_init) {
		chunk.Destroy();
	}
	//! Reinitialize for every new scan, the schema isn't guaranteed to be the same for every scan
	//! the 'partition' of the 'data_file' is based on the partition spec referenced by the manifest
	scan->InitializeChunk(chunk);

	finished = false;
	offset = 0;
}

const IcebergAvroScanInfo &BaseManifestReader::GetScanInfo() const {
	return *scan->scan_info;
}

idx_t BaseManifestReader::ScanInternal(idx_t remaining) {
	if (!scan || finished) {
		return 0;
	}

	if (offset >= chunk.size()) {
		scan->GetNext(chunk);
		if (chunk.size() == 0) {
			finished = true;
			return 0;
		}
		offset = 0;
	}
	return MinValue(chunk.size() - offset, remaining);
}

bool BaseManifestReader::Finished() const {
	if (!scan) {
		return true;
	}
	return scan->Finished();
}

} // namespace duckdb
