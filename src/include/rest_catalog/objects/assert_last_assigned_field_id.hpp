
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

class AssertLastAssignedFieldId {
public:
	AssertLastAssignedFieldId();
	AssertLastAssignedFieldId(const AssertLastAssignedFieldId &) = delete;
	AssertLastAssignedFieldId &operator=(const AssertLastAssignedFieldId &) = delete;
	AssertLastAssignedFieldId(AssertLastAssignedFieldId &&) = default;
	AssertLastAssignedFieldId &operator=(AssertLastAssignedFieldId &&) = default;

public:
	static AssertLastAssignedFieldId FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	TableRequirement table_requirement;
	int64_t last_assigned_field_id;
	string type;
	bool has_type = false;
};

} // namespace rest_api_objects
} // namespace duckdb
