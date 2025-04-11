
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/view_version.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateViewRequest {
public:
	CreateViewRequest();
	CreateViewRequest(const CreateViewRequest &) = delete;
	CreateViewRequest &operator=(const CreateViewRequest &) = delete;
	CreateViewRequest(CreateViewRequest &&) = default;
	CreateViewRequest &operator=(CreateViewRequest &&) = default;

public:
	static CreateViewRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string name;
	Schema schema;
	ViewVersion view_version;
	case_insensitive_map_t<string> properties;
	string location;
	bool has_location = false;
};

} // namespace rest_api_objects
} // namespace duckdb
