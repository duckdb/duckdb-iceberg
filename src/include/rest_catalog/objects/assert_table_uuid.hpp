
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

class AssertTableUUID {
public:
	AssertTableUUID();
	AssertTableUUID(const AssertTableUUID &) = delete;
	AssertTableUUID &operator=(const AssertTableUUID &) = delete;
	AssertTableUUID(AssertTableUUID &&) = default;
	AssertTableUUID &operator=(AssertTableUUID &&) = default;

public:
	static AssertTableUUID FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	TableRequirement table_requirement;
	string type;
	string uuid;
};

} // namespace rest_api_objects
} // namespace duckdb
