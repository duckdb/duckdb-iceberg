
#include "rest_catalog/objects/remove_partition_specs_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemovePartitionSpecsUpdate::RemovePartitionSpecsUpdate() {
}

RemovePartitionSpecsUpdate RemovePartitionSpecsUpdate::FromJSON(yyjson_val *obj) {
	RemovePartitionSpecsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RemovePartitionSpecsUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto spec_ids_val = yyjson_obj_get(obj, "spec-ids");
	if (!spec_ids_val) {
		return "RemovePartitionSpecsUpdate required property 'spec-ids' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(spec_ids_val, idx, max, val) {
			auto tmp = yyjson_get_sint(val);
			spec_ids.emplace_back(std::move(tmp));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		action = yyjson_get_str(action_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
