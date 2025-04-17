
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/reference.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TransformTerm {
public:
	TransformTerm();
	TransformTerm(const TransformTerm &) = delete;
	TransformTerm &operator=(const TransformTerm &) = delete;
	TransformTerm(TransformTerm &&) = default;
	TransformTerm &operator=(TransformTerm &&) = default;

public:
	static TransformTerm FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string type;
	Transform transform;
	Reference term;
};

} // namespace rest_api_objects
} // namespace duckdb
