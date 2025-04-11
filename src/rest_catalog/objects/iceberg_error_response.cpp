
#include "rest_catalog/objects/iceberg_error_response.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

IcebergErrorResponse::IcebergErrorResponse() {
}

IcebergErrorResponse IcebergErrorResponse::FromJSON(yyjson_val *obj) {
	IcebergErrorResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string IcebergErrorResponse::TryFromJSON(yyjson_val *obj) {
	string error;
	auto _error_val = yyjson_obj_get(obj, "error");
	if (!_error_val) {
		return "IcebergErrorResponse required property 'error' is missing";
	} else {
		error = _error.TryFromJSON(_error_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
