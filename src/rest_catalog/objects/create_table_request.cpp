
#include "rest_catalog/objects/create_table_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CreateTableRequest::CreateTableRequest() {
}

CreateTableRequest CreateTableRequest::FromJSON(yyjson_val *obj) {
	CreateTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CreateTableRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto name_val = yyjson_obj_get(obj, "name");
	if (!name_val) {
		return "CreateTableRequest required property 'name' is missing";
	} else {
		name = yyjson_get_str(name_val);
	}
	auto schema_val = yyjson_obj_get(obj, "schema");
	if (!schema_val) {
		return "CreateTableRequest required property 'schema' is missing";
	} else {
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (location_val) {
		has_location = true;
		location = yyjson_get_str(location_val);
	}
	auto partition_spec_val = yyjson_obj_get(obj, "partition-spec");
	if (partition_spec_val) {
		has_partition_spec = true;
		error = partition_spec.TryFromJSON(partition_spec_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto write_order_val = yyjson_obj_get(obj, "write-order");
	if (write_order_val) {
		has_write_order = true;
		error = write_order.TryFromJSON(write_order_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto stage_create_val = yyjson_obj_get(obj, "stage-create");
	if (stage_create_val) {
		has_stage_create = true;
		stage_create = yyjson_get_bool(stage_create_val);
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		properties = parse_object_of_strings(properties_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
