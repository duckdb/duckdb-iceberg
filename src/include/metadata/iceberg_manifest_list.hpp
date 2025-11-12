#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "metadata/iceberg_manifest.hpp"

#include "duckdb/function/copy_function.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

using sequence_number_t = int64_t;

struct FieldSummary {
public:
	Value ToValue() const {
		child_list_t<Value> children;
		children.emplace_back("contains_null", Value::BOOLEAN(contains_null));
		children.emplace_back("contains_nan", Value::BOOLEAN(contains_nan));
		D_ASSERT(lower_bound.type().id() == LogicalType::BLOB);
		D_ASSERT(upper_bound.type().id() == LogicalType::BLOB);
		children.emplace_back("lower_bound", lower_bound);
		children.emplace_back("upper_bound", upper_bound);
		return Value::STRUCT(children);
	}

public:
	bool contains_null = false;
	//! Optional
	bool contains_nan = false;
	//! Optional
	Value lower_bound;
	//! Optional
	Value upper_bound;
};

struct ManifestPartitions {
public:
	Value ToValue() const {
		child_list_t<LogicalType> children;
		children.emplace_back("contains_null", LogicalType::BOOLEAN);
		children.emplace_back("contains_nan", LogicalType::BOOLEAN);
		children.emplace_back("lower_bound", LogicalType::BLOB);
		children.emplace_back("upper_bound", LogicalType::BLOB);
		auto field_summary_struct = LogicalType::STRUCT(children);

		if (!has_partitions) {
			return Value(LogicalType::LIST(field_summary_struct));
		}
		vector<Value> fields;
		for (auto &field : field_summary) {
			fields.push_back(field.ToValue());
		}
		return Value::LIST(field_summary_struct, fields);
	}

public:
	bool has_partitions = false;
	vector<FieldSummary> field_summary;
};

enum class IcebergManifestContentType : uint8_t {
	DATA = 0,
	DELETE = 1,
};

//! An entry in the manifest list file (top level AVRO file)
struct IcebergManifestListEntry {
public:
	//! Path to the manifest AVRO file
	string manifest_path;
	//! Length of the manifest file in bytes
	int64_t manifest_length;
	//! sequence_number when manifest was added to table (0 for Iceberg v1)
	sequence_number_t sequence_number = 0;
	bool has_min_sequence_number = false;
	sequence_number_t min_sequence_number = 0;
	//! either data or deletes
	IcebergManifestContentType content;
	int64_t added_snapshot_id = -1;
	//! added files count
	idx_t added_files_count = 0;
	//! existing files count
	idx_t existing_files_count = 0;
	//! deleted files count
	idx_t deleted_files_count = 0;
	//! added rows in the manifest
	idx_t added_rows_count = 0;
	//! existing rows in the manifest
	idx_t existing_rows_count = 0;
	//! deleted rows in the manifest
	idx_t deleted_rows_count = 0;
	//! The id of the partition spec referenced by this manifest (and the data files that are part of it)
	int32_t partition_spec_id;
	//! The field summaries of the partition (if present)
	ManifestPartitions partitions;
	//! the actual manifest file information
	IcebergManifestFile manifest_file;

public:
	IcebergManifestListEntry(string manifest_path) : manifest_path(manifest_path), manifest_file(manifest_path) {
	}

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

struct IcebergManifestList {
public:
	IcebergManifestList(const string &path) : path(path) {
	}

	vector<IcebergManifestListEntry> &GetManifestFilesMutable();
	const vector<IcebergManifestListEntry> &GetManifestFilesConst() const;

	IcebergManifestListEntry &CreateNewManifestListEntry(string manifest_file_path) {
		manifest_entries.push_back(IcebergManifestListEntry(manifest_file_path));
		return manifest_entries.back();
	}
	idx_t GetManifestListEntriesCount() const;

	void WriteManifestListEntry(IcebergTableInformation &table_info, idx_t manifest_index, CopyFunction &avro_copy,
	                            DatabaseInstance &db, ClientContext &context);
	void AddToManifestEntries(vector<IcebergManifestListEntry> &manifest_list_entries);
	vector<IcebergManifestListEntry> GetManifestListEntries();

public:
	string path;

private:
	vector<IcebergManifestListEntry> manifest_entries;
};

namespace manifest_list {

static constexpr const int32_t MANIFEST_PATH = 500;
static constexpr const int32_t MANIFEST_LENGTH = 501;
static constexpr const int32_t PARTITION_SPEC_ID = 502;
static constexpr const int32_t CONTENT = 517;
static constexpr const int32_t SEQUENCE_NUMBER = 515;
static constexpr const int32_t MIN_SEQUENCE_NUMBER = 516;
static constexpr const int32_t ADDED_SNAPSHOT_ID = 503;
static constexpr const int32_t ADDED_FILES_COUNT = 504;
static constexpr const int32_t EXISTING_FILES_COUNT = 505;
static constexpr const int32_t DELETED_FILES_COUNT = 506;
static constexpr const int32_t ADDED_ROWS_COUNT = 512;
static constexpr const int32_t EXISTING_ROWS_COUNT = 513;
static constexpr const int32_t DELETED_ROWS_COUNT = 514;
static constexpr const int32_t PARTITIONS = 507;
static constexpr const int32_t PARTITIONS_ELEMENT = 508;
static constexpr const int32_t FIELD_SUMMARY_CONTAINS_NULL = 509;
static constexpr const int32_t FIELD_SUMMARY_CONTAINS_NAN = 518;
static constexpr const int32_t FIELD_SUMMARY_LOWER_BOUND = 510;
static constexpr const int32_t FIELD_SUMMARY_UPPER_BOUND = 511;
static constexpr const int32_t KEY_METADATA = 519;
static constexpr const int32_t FIRST_ROW_ID = 520;

void WriteToFile(const IcebergManifestList &manifest_list, CopyFunction &copy_function, DatabaseInstance &db,
                 ClientContext &context);

} // namespace manifest_list

} // namespace duckdb
