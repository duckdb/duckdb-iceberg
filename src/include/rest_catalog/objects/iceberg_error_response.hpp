#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/error_model.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class IcebergErrorResponse {
public:
	static IcebergErrorResponse FromJSON(yyjson_val *obj) {
		IcebergErrorResponse result;
		auto error_val = yyjson_obj_get(obj, "error");
		if (error_val) {
			result.error = ErrorModel::FromJSON(error_val);
		}
		return result;
	}
public:
	ErrorModel error;
};

} // namespace rest_api_objects
} // namespace duckdb