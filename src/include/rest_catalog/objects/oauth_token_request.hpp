#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class OAuthTokenRequest {
public:
	static OAuthTokenRequest FromJSON(yyjson_val *obj) {
		OAuthTokenRequest result;

		return result;
	}

public:
};
} // namespace rest_api_objects
} // namespace duckdb