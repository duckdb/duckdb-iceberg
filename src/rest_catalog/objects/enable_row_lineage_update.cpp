
#include "rest_catalog/objects/enable_row_lineage_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

EnableRowLineageUpdate::EnableRowLineageUpdate() {
}

EnableRowLineageUpdate EnableRowLineageUpdate::FromJSON(yyjson_val *obj) {
	EnableRowLineageUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string EnableRowLineageUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return "EnableRowLineageUpdate property 'action' is not of type 'string'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
