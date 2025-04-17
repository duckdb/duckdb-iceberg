
#include "rest_catalog/objects/base_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

BaseUpdate::BaseUpdate() {
}

BaseUpdate BaseUpdate::FromJSON(yyjson_val *obj) {
	BaseUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string BaseUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	auto action_val = yyjson_obj_get(obj, "action");
	if (!action_val) {
		return "BaseUpdate required property 'action' is missing";
	} else {
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format("BaseUpdate property 'action' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(action_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
