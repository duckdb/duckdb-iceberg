#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"

#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergManifestFile;

using sequence_number_t = int64_t;

enum class IcebergManifestEntryContentType : uint8_t { DATA = 0, POSITION_DELETES = 1, EQUALITY_DELETES = 2 };

enum class IcebergManifestEntryStatusType : uint8_t { EXISTING = 0, ADDED = 1, DELETED = 2 };

string IcebergManifestEntryContentTypeToString(IcebergManifestEntryContentType type);
string IcebergManifestEntryStatusTypeToString(IcebergManifestEntryStatusType type);

//! Combined partition information for a single partition field in a data file
struct DataFilePartitionInfo {
	//! The partition column name
	string name;
	//! The source column id from the table schema
	uint64_t source_id = 0;
	//! The partition field id (unique within a partition spec)
	uint64_t field_id = 0;
	//! The partition transform applied to the source column
	IcebergTransform transform;
	//! The source column type from the table schema
	LogicalType source_type;
	//! The actual partition value for this data file
	Value value;

	bool operator==(const DataFilePartitionInfo &other) const {
		return field_id == other.field_id && value == other.value;
	}
	bool operator!=(const DataFilePartitionInfo &other) const {
		return !(*this == other);
	}
};

struct IcebergDataFile {
public:
	Value ToValue(const IcebergTableMetadata &table_metadata, const LogicalType &type) const;

public:
	static map<idx_t, LogicalType> GetFieldIdToTypeMapping(const IcebergSnapshot &snapshot,
	                                                       const IcebergTableMetadata &metadata,
	                                                       const unordered_set<int32_t> &partition_spec_ids);
	static LogicalType PartitionStructType(const map<idx_t, LogicalType> &partition_field_id_to_type);
	static LogicalType GetType(const IcebergTableMetadata &metadata, const LogicalType &partition_type);

public:
	void SetFirstRowId(int64_t first_row_id);
	bool HasFirstRowId() const;
	int64_t GetFirstRowId() const;

public:
	IcebergManifestEntryContentType content;
	string file_path;
	string file_format;
	//! Combined partition information in partition spec order.
	//! Contains name, source_id, field_id, transform, source_type, and the actual partition value.
	vector<DataFilePartitionInfo> partition_info;
	int64_t record_count;

	int64_t file_size_in_bytes;
	unordered_map<int32_t, int64_t> column_sizes;
	unordered_map<int32_t, int64_t> value_counts;
	unordered_map<int32_t, int64_t> null_value_counts;
	unordered_map<int32_t, int64_t> nan_value_counts;
	//! source_id -> blob
	unordered_map<int32_t, Value> lower_bounds;
	unordered_map<int32_t, Value> upper_bounds;
	vector<int32_t> equality_ids;
	vector<int64_t> split_offsets;

	bool has_sort_order_id = false;
	int32_t sort_order_id;

	string referenced_data_file;
	Value content_offset;
	Value content_size_in_bytes;

private:
	bool has_first_row_id = false;
	int64_t first_row_id = 0xDEADBEEF;
};

//! An entry in a manifest file
struct IcebergManifestEntry {
public:
	IcebergManifestEntryStatusType status;
	IcebergDataFile data_file;

public:
	void SetSnapshotId(int64_t snapshot_id);
	bool HasSnapshotId() const;
	int64_t GetSnapshotId() const;
	void SetSequenceNumber(sequence_number_t value);
	void SetFileSequenceNumber(sequence_number_t value);
	sequence_number_t GetSequenceNumber(const IcebergManifestFile &manifest_file) const;
	sequence_number_t GetFileSequenceNumber(const IcebergManifestFile &manifest_file) const;

private:
	bool has_snapshot_id = false;
	int64_t snapshot_id;

	bool has_sequence_number = false;
	sequence_number_t sequence_number;

	bool has_file_sequence_number = false;
	sequence_number_t file_sequence_number;
};

struct IcebergManifestListEntry;

namespace manifest_file {

static constexpr const int32_t STATUS = 0;
static constexpr const int32_t SNAPSHOT_ID = 1;
static constexpr const int32_t SEQUENCE_NUMBER = 3;
static constexpr const int32_t FILE_SEQUENCE_NUMBER = 4;
static constexpr const int32_t DATA_FILE = 2;
static constexpr const int32_t CONTENT = 134;
static constexpr const int32_t FILE_PATH = 100;
static constexpr const int32_t FILE_FORMAT = 101;
static constexpr const int32_t PARTITION = 102;
static constexpr const int32_t RECORD_COUNT = 103;
static constexpr const int32_t FILE_SIZE_IN_BYTES = 104;
// static constexpr const int32_t BLOCK_SIZE_IN_BYTES = 105; // (deprecated)
// static constexpr const int32_t FILE_ORDINAL = 106; // (deprecated)
// static constexpr const int32_t SORT_COLUMNS = 107; // (deprecated)
// static constexpr const int32_t SORT_COLUMNS_ELEMENT = 112; // (deprecated)
static constexpr const int32_t COLUMN_SIZES = 108;
static constexpr const int32_t COLUMN_SIZES_KEY = 117;
static constexpr const int32_t COLUMN_SIZES_VALUE = 118;
static constexpr const int32_t VALUE_COUNTS = 109;
static constexpr const int32_t VALUE_COUNTS_KEY = 119;
static constexpr const int32_t VALUE_COUNTS_VALUE = 120;
static constexpr const int32_t NULL_VALUE_COUNTS = 110;
static constexpr const int32_t NULL_VALUE_COUNTS_KEY = 121;
static constexpr const int32_t NULL_VALUE_COUNTS_VALUE = 122;
static constexpr const int32_t NAN_VALUE_COUNTS = 137;
static constexpr const int32_t NAN_VALUE_COUNTS_KEY = 138;
static constexpr const int32_t NAN_VALUE_COUNTS_VALUE = 139;
// static constexpr const int32_t DISTINCT_COUNTS = 111; // (deprecated)
static constexpr const int32_t LOWER_BOUNDS = 125;
static constexpr const int32_t LOWER_BOUNDS_KEY = 126;
static constexpr const int32_t LOWER_BOUNDS_VALUE = 127;
static constexpr const int32_t UPPER_BOUNDS = 128;
static constexpr const int32_t UPPER_BOUNDS_KEY = 129;
static constexpr const int32_t UPPER_BOUNDS_VALUE = 130;
// static constexpr const int32_t KEY_METADATA = 131; // (optional)
static constexpr const int32_t SPLIT_OFFSETS = 132;
static constexpr const int32_t SPLIT_OFFSETS_ELEMENT = 133;
static constexpr const int32_t EQUALITY_IDS = 135;
static constexpr const int32_t EQUALITY_IDS_ELEMENT = 136;
static constexpr const int32_t SORT_ORDER_ID = 140;
static constexpr const int32_t FIRST_ROW_ID = 142;
static constexpr const int32_t REFERENCED_DATA_FILE = 143;
static constexpr const int32_t CONTENT_OFFSET = 144;
static constexpr const int32_t CONTENT_SIZE_IN_BYTES = 145;

idx_t WriteToFile(const IcebergTableMetadata &table_metadata, const IcebergManifestFile &manifest_file,
                  const vector<IcebergManifestEntry> &entries, CopyFunction &copy_function, DatabaseInstance &db,
                  ClientContext &context);

} // namespace manifest_file

} // namespace duckdb
