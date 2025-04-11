
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/reference.hpp"
#include "rest_catalog/objects/transform_term.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Term {
public:
	Term();
	Term(const Term &) = delete;
	Term &operator=(const Term &) = delete;
	Term(Term &&) = default;
	Term &operator=(Term &&) = default;

public:
	static Term FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	Reference reference;
	bool has_reference = false;
	TransformTerm transform_term;
	bool has_transform_term = false;
};

} // namespace rest_api_objects
} // namespace duckdb
