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

#include "metadata/iceberg_table_schema.hpp"

namespace duckdb {

struct IcebergTableInformation;

using sequence_number_t = int64_t;

enum class IcebergManifestEntryContentType : uint8_t { DATA = 0, POSITION_DELETES = 1, EQUALITY_DELETES = 2 };

enum class IcebergManifestEntryStatusType : uint8_t { EXISTING = 0, ADDED = 1, DELETED = 2 };

//! An entry in a manifest file
struct IcebergManifestEntry {
public:
	IcebergManifestEntryStatusType status;
	//! ----- Data File Struct ------
	IcebergManifestEntryContentType content;
	string file_path;
	string file_format;
	vector<int32_t> equality_ids;
	int64_t record_count;
	//! source_id -> blob
	unordered_map<int32_t, Value> lower_bounds;
	unordered_map<int32_t, Value> upper_bounds;
	unordered_map<int32_t, int64_t> column_sizes;
	unordered_map<int32_t, int64_t> value_counts;
	unordered_map<int32_t, int64_t> null_value_counts;
	unordered_map<int32_t, int64_t> nan_value_counts;
	vector<pair<int32_t, Value>> partition_values;
	//! Inherited from the 'manifest_file' if NULL and 'status == EXISTING'
	sequence_number_t sequence_number;
	int64_t snapshot_id;
	//! Inherited from the 'manifest_file'
	int32_t partition_spec_id;
	int64_t file_size_in_bytes;
	string referenced_data_file;
	Value content_offset;
	Value content_size_in_bytes;

public:
	Value ToDataFileStruct(const LogicalType &type) const;

public:
	static vector<LogicalType> Types() {
		return {
		    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
		};
	}

	static string ContentTypeToString(IcebergManifestEntryContentType type) {
		switch (type) {
		case IcebergManifestEntryContentType::DATA:
			return "EXISTING";
		case IcebergManifestEntryContentType::POSITION_DELETES:
			return "POSITION_DELETES";
		case IcebergManifestEntryContentType::EQUALITY_DELETES:
			return "EQUALITY_DELETES";
		default:
			throw InvalidConfigurationException("Invalid Manifest Entry Content Type");
		}
	}

	static string StatusTypeToString(IcebergManifestEntryStatusType type) {
		switch (type) {
		case IcebergManifestEntryStatusType::EXISTING:
			return "EXISTING";
		case IcebergManifestEntryStatusType::ADDED:
			return "ADDED";
		case IcebergManifestEntryStatusType::DELETED:
			return "DELETED";
		default:
			throw InvalidConfigurationException("Invalid matifest entry type");
		}
	}

	static vector<string> Names() {
		return {"status", "content", "file_path", "file_format", "record_count"};
	}
};

struct IcebergManifestFile {
	IcebergManifestFile(const string &path) : path(path) {
	}

public:
	string path;
	vector<IcebergManifestEntry> data_files;
};

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

idx_t WriteToFile(IcebergTableInformation &table_info, const IcebergManifestFile &manifest_file,
                  CopyFunction &copy_function, DatabaseInstance &db, ClientContext &context);

} // namespace manifest_file

} // namespace duckdb
