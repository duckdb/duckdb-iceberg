
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/assert_view_uuid.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewRequirement {
public:
	ViewRequirement();
	ViewRequirement(const ViewRequirement &) = delete;
	ViewRequirement &operator=(const ViewRequirement &) = delete;
	ViewRequirement(ViewRequirement &&) = default;
	ViewRequirement &operator=(ViewRequirement &&) = default;

public:
	static ViewRequirement FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	AssertViewUUID assert_view_uuid;
	bool has_assert_view_uuid = false;
};

} // namespace rest_api_objects
} // namespace duckdb
