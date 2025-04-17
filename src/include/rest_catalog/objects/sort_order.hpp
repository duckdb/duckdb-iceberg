
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/sort_field.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SortOrder {
public:
	SortOrder();
	SortOrder(const SortOrder &) = delete;
	SortOrder &operator=(const SortOrder &) = delete;
	SortOrder(SortOrder &&) = default;
	SortOrder &operator=(SortOrder &&) = default;

public:
	static SortOrder FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t order_id;
	vector<SortField> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
