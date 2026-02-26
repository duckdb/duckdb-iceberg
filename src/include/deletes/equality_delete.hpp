#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/planner/expression.hpp"
#include "metadata/iceberg_manifest.hpp"

namespace duckdb {

using sequence_number_t = int64_t;

struct IcebergEqualityDeleteRow {
public:
	IcebergEqualityDeleteRow() {
	}
	IcebergEqualityDeleteRow(const IcebergEqualityDeleteRow &) = delete;
	IcebergEqualityDeleteRow &operator=(const IcebergEqualityDeleteRow &) = delete;
	IcebergEqualityDeleteRow(IcebergEqualityDeleteRow &&) = default;
	IcebergEqualityDeleteRow &operator=(IcebergEqualityDeleteRow &&) = default;

public:
	//! Map of field-id to equality delete for the field
	//! NOTE: these are either OPERATOR_IS_NULL or COMPARE_EQUAL
	//! Also note: it's probably easiest to apply these to the 'output_chunk' of FinalizeChunk, so we can re-use
	//! expressions. Otherwise the idx of the BoundReferenceExpression would have to change for every file.
	unordered_map<int32_t, unique_ptr<Expression>> filters;
};

struct IcebergEqualityDeleteFile {
public:
	IcebergEqualityDeleteFile(vector<DataFilePartitionInfo> partition_info_p, int32_t partition_spec_id)
	    : partition_info(std::move(partition_info_p)), partition_spec_id(partition_spec_id) {
	}

public:
	//! The partition info if the equality delete has partition information
	vector<DataFilePartitionInfo> partition_info;
	int32_t partition_spec_id;
	vector<IcebergEqualityDeleteRow> rows;
};

struct IcebergEqualityDeleteData {
public:
	IcebergEqualityDeleteData(sequence_number_t sequence_number) : sequence_number(sequence_number) {
	}

public:
	sequence_number_t sequence_number;
	vector<IcebergEqualityDeleteFile> files;
};

} // namespace duckdb
