
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionField {
public:
	PartitionField();
	PartitionField(const PartitionField &) = delete;
	PartitionField &operator=(const PartitionField &) = delete;
	PartitionField(PartitionField &&) = default;
	PartitionField &operator=(PartitionField &&) = default;

public:
	static PartitionField FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t source_id;
	Transform transform;
	string name;
	int64_t field_id;
	bool has_field_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
