
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/schema.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AddSchemaUpdate {
public:
	AddSchemaUpdate();
	AddSchemaUpdate(const AddSchemaUpdate &) = delete;
	AddSchemaUpdate &operator=(const AddSchemaUpdate &) = delete;
	AddSchemaUpdate(AddSchemaUpdate &&) = default;
	AddSchemaUpdate &operator=(AddSchemaUpdate &&) = default;

public:
	static AddSchemaUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	Schema schema;
	string action;
	bool has_action = false;
	int64_t last_column_id;
	bool has_last_column_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
