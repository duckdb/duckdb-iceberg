
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/table_requirement_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertDefaultSortOrderId {
public:
	AssertDefaultSortOrderId();
	AssertDefaultSortOrderId(const AssertDefaultSortOrderId &) = delete;
	AssertDefaultSortOrderId &operator=(const AssertDefaultSortOrderId &) = delete;
	AssertDefaultSortOrderId(AssertDefaultSortOrderId &&) = default;
	AssertDefaultSortOrderId &operator=(AssertDefaultSortOrderId &&) = default;

public:
	static AssertDefaultSortOrderId FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	TableRequirementType type;
	int32_t default_sort_order_id;
};

} // namespace rest_api_objects
} // namespace duckdb
