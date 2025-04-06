
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/iceberg_error_response.hpp"
#include "rest_catalog/objects/plan_status.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FailedPlanningResult {
public:
	FailedPlanningResult();
	class Object7 {
	public:
		Object7();

	public:
		static Object7 FromJSON(yyjson_val *obj);

	public:
		string TryFromJSON(yyjson_val *obj);

	public:
		PlanStatus status;
	};

public:
	static FailedPlanningResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	IcebergErrorResponse iceberg_error_response;
	Object7 object_7;
};

} // namespace rest_api_objects
} // namespace duckdb
