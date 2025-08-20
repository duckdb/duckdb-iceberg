
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/table_requirement_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertDefaultSpecId {
public:
	AssertDefaultSpecId();
	AssertDefaultSpecId(const AssertDefaultSpecId &) = delete;
	AssertDefaultSpecId &operator=(const AssertDefaultSpecId &) = delete;
	AssertDefaultSpecId(AssertDefaultSpecId &&) = default;
	AssertDefaultSpecId &operator=(AssertDefaultSpecId &&) = default;

public:
	static AssertDefaultSpecId FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	TableRequirementType type;
	int32_t default_spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
