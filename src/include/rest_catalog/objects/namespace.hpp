
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Namespace {
public:
	Namespace();
	Namespace(const Namespace &) = delete;
	Namespace &operator=(const Namespace &) = delete;
	Namespace(Namespace &&) = default;
	Namespace &operator=(Namespace &&) = default;

public:
	static Namespace FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<string> value;
};

} // namespace rest_api_objects
} // namespace duckdb
