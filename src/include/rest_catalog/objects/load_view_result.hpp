
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/view_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class LoadViewResult {
public:
	LoadViewResult();
	LoadViewResult(const LoadViewResult &) = delete;
	LoadViewResult &operator=(const LoadViewResult &) = delete;
	LoadViewResult(LoadViewResult &&) = default;
	LoadViewResult &operator=(LoadViewResult &&) = default;

public:
	static LoadViewResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string metadata_location;
	ViewMetadata metadata;
	case_insensitive_map_t<string> config;
	bool has_config = false;
};

} // namespace rest_api_objects
} // namespace duckdb
