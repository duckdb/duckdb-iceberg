
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePartitionSpecsUpdate {
public:
	RemovePartitionSpecsUpdate();
	RemovePartitionSpecsUpdate(const RemovePartitionSpecsUpdate &) = delete;
	RemovePartitionSpecsUpdate &operator=(const RemovePartitionSpecsUpdate &) = delete;
	RemovePartitionSpecsUpdate(RemovePartitionSpecsUpdate &&) = default;
	RemovePartitionSpecsUpdate &operator=(RemovePartitionSpecsUpdate &&) = default;

public:
	static RemovePartitionSpecsUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	vector<int64_t> spec_ids;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
