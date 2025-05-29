//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

using sequence_number_t = int64_t;

struct FieldSummary {
	bool contains_null = false;
	//! Optional
	bool contains_nan = false;
	//! Optional
	Value lower_bound;
	//! Optional
	Value upper_bound;
};

struct ManifestPartitions {
	bool has_partitions = false;
	vector<FieldSummary> field_summary;
};

enum class IcebergManifestContentType : uint8_t {
	DATA = 0,
	DELETE = 1,
};

//! An entry in the manifest list file (top level AVRO file)
struct IcebergManifest {
public:
	//! Path to the manifest AVRO file
	string manifest_path;
	//! sequence_number when manifest was added to table (0 for Iceberg v1)
	sequence_number_t sequence_number;
	//! either data or deletes
	IcebergManifestContentType content;
	//! added rows in the manifest
	idx_t added_rows_count = 0;
	//! existing rows in the manifest
	idx_t existing_rows_count = 0;
	//! The id of the partition spec referenced by this manifest (and the data files that are part of it)
	int32_t partition_spec_id;
	//! The field summaries of the partition (if present)
	ManifestPartitions partitions;

public:
	static vector<LogicalType> Types() {
		return {
		    LogicalType::VARCHAR,
		    LogicalType::BIGINT,
		    LogicalType::VARCHAR,
		};
	}

	static string ContentTypeToString(IcebergManifestContentType type) {
		switch (type) {
		case IcebergManifestContentType::DATA:
			return "DATA";
		case IcebergManifestContentType::DELETE:
			return "DELETE";
		default:
			throw InvalidConfigurationException("Invalid Manifest Content Type");
		}
	}

	static vector<string> Names() {
		return {"manifest_path", "manifest_sequence_number", "manifest_content"};
	}
};

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
	unordered_map<int32_t, int64_t> value_counts;
	unordered_map<int32_t, int64_t> null_value_counts;
	unordered_map<int32_t, int64_t> nan_value_counts;
	Value partition;
	//! Inherited from the 'manifest_file' if NULL and 'status == EXISTING'
	sequence_number_t sequence_number;
	//! Inherited from the 'manifest_file'
	int32_t partition_spec_id;
	int64_t file_size_in_bytes;

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

struct IcebergTableEntry {
	IcebergManifest manifest;
	vector<IcebergManifestEntry> manifest_entries;
};

} // namespace duckdb
