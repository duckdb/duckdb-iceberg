
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PageToken {
public:
	PageToken();
	PageToken(const PageToken &) = delete;
	PageToken &operator=(const PageToken &) = delete;
	PageToken(PageToken &&) = default;
	PageToken &operator=(PageToken &&) = default;

public:
	static PageToken FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
