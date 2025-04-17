
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
		if (yyjson_is_str(name_val)) {
			name = yyjson_get_str(name_val);
		} else {
			return StringUtil::Format("CreateTableRequest property 'name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(name_val));
		}
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
		if (yyjson_is_str(location_val)) {
			location = yyjson_get_str(location_val);
		} else {
			return StringUtil::Format(
			    "CreateTableRequest property 'location' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(location_val));
		}
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
		if (yyjson_is_bool(stage_create_val)) {
			stage_create = yyjson_get_bool(stage_create_val);
		} else {
			return StringUtil::Format(
			    "CreateTableRequest property 'stage_create' is not of type 'boolean', found '%s' instead",
			    yyjson_get_type_desc(stage_create_val));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "CreateTableRequest property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				properties.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CreateTableRequest property 'properties' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
