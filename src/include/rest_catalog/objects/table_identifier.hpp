
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/namespace.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableIdentifier {
public:
	TableIdentifier();
	TableIdentifier(const TableIdentifier &) = delete;
	TableIdentifier &operator=(const TableIdentifier &) = delete;
	TableIdentifier(TableIdentifier &&) = default;
	TableIdentifier &operator=(TableIdentifier &&) = default;

public:
	static TableIdentifier FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	Namespace _namespace;
	string name;
};

} // namespace rest_api_objects
} // namespace duckdb
