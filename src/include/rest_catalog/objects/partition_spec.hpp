
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/partition_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionSpec {
public:
	PartitionSpec();

public:
	static PartitionSpec FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<PartitionField> fields;
	int64_t spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
