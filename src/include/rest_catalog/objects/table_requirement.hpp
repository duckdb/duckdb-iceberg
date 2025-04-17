
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableRequirement {
public:
	TableRequirement();
	TableRequirement(const TableRequirement &) = delete;
	TableRequirement &operator=(const TableRequirement &) = delete;
	TableRequirement(TableRequirement &&) = default;
	TableRequirement &operator=(TableRequirement &&) = default;

public:
	static TableRequirement FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
