
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertCreate {
public:
	AssertCreate();
	AssertCreate(const AssertCreate &) = delete;
	AssertCreate &operator=(const AssertCreate &) = delete;
	AssertCreate(AssertCreate &&) = default;
	AssertCreate &operator=(AssertCreate &&) = default;

public:
	static AssertCreate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	TableRequirement table_requirement;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
