
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/sort_order.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CreateTableRequest {
public:
	CreateTableRequest();
	CreateTableRequest(const CreateTableRequest &) = delete;
	CreateTableRequest &operator=(const CreateTableRequest &) = delete;
	CreateTableRequest(CreateTableRequest &&) = default;
	CreateTableRequest &operator=(CreateTableRequest &&) = default;

public:
	static CreateTableRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string name;
	Schema schema;
	string location;
	bool has_location = false;
	PartitionSpec partition_spec;
	bool has_partition_spec = false;
	SortOrder write_order;
	bool has_write_order = false;
	bool stage_create;
	bool has_stage_create = false;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
};

} // namespace rest_api_objects
} // namespace duckdb
