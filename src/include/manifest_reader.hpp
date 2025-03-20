#pragma once

#include "iceberg_options.hpp"
#include "iceberg_types.hpp"
#include "iceberg_manifest.hpp"

namespace duckdb {

// Manifest Reader

typedef void (*manifest_reader_name_mapping)(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &mapping);
typedef bool (*manifest_reader_schema_validation)(const case_insensitive_map_t<ColumnIndex> &mapping);

typedef idx_t (*manifest_reader_manifest_producer)(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input, vector<IcebergManifest> &result);
typedef idx_t (*manifest_reader_manifest_entry_producer)(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input, vector<IcebergManifestEntry> &result);

using manifest_reader_read = std::function<idx_t(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input)>;

struct ManifestReaderInput {
public:
	ManifestReaderInput(const case_insensitive_map_t<ColumnIndex> &name_to_vec, bool skip_deleted = false);
public:
	const case_insensitive_map_t<ColumnIndex> &name_to_vec;
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
};

class ManifestReader {
public:
	ManifestReader(manifest_reader_name_mapping name_mapping, manifest_reader_schema_validation schema_validator);
public:
	void Initialize(unique_ptr<AvroScan> scan_p);
public:
	bool Finished() const;
	idx_t ReadEntries(idx_t count, manifest_reader_read callback);
private:
	unique_ptr<AvroScan> scan;
	DataChunk chunk;
	idx_t offset = 0;
	bool finished = true;
	case_insensitive_map_t<ColumnIndex> name_to_vec;

	manifest_reader_name_mapping name_mapping = nullptr;
	manifest_reader_schema_validation schema_validation = nullptr;
public:
	bool skip_deleted = false;
};


template <class OP>
vector<typename OP::entry_type> ScanAvroMetadata(const string &scan_name, ClientContext &context, const string &path) {
	auto scan = make_uniq<AvroScan>(scan_name, context, path);

	ManifestReader manifest_reader(OP::PopulateNameMapping, OP::VerifySchema);
	manifest_reader.Initialize(std::move(scan));
	auto manifest_producer = OP::ProduceEntries;

	vector<typename OP::entry_type> ret;
	while (!manifest_reader.Finished()) {
		manifest_reader.ReadEntries(STANDARD_VECTOR_SIZE, [&ret, manifest_producer](DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input) {
			return manifest_producer(chunk, offset, count, input, ret);
		});
	}
	return ret;
}

} // namespace duckdb
