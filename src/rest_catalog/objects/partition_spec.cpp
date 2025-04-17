
#include "rest_catalog/objects/partition_spec.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PartitionSpec::PartitionSpec() {
}

PartitionSpec PartitionSpec::FromJSON(yyjson_val *obj) {
	PartitionSpec res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PartitionSpec::TryFromJSON(yyjson_val *obj) {
	string error;
	auto fields_val = yyjson_obj_get(obj, "fields");
	if (!fields_val) {
		return "PartitionSpec required property 'fields' is missing";
	} else {
		if (yyjson_is_arr(fields_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				PartitionField tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				fields.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("PartitionSpec property 'fields' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(fields_val));
		}
	}
	auto spec_id_val = yyjson_obj_get(obj, "spec-id");
	if (spec_id_val) {
		has_spec_id = true;
		if (yyjson_is_int(spec_id_val)) {
			spec_id = yyjson_get_int(spec_id_val);
		} else {
			return StringUtil::Format("PartitionSpec property 'spec_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(spec_id_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
