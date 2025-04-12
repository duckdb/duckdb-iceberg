
#include "rest_catalog/objects/view_history_entry.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ViewHistoryEntry::ViewHistoryEntry() {
}

ViewHistoryEntry ViewHistoryEntry::FromJSON(yyjson_val *obj) {
	ViewHistoryEntry res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ViewHistoryEntry::TryFromJSON(yyjson_val *obj) {
	string error;
	auto version_id_val = yyjson_obj_get(obj, "version-id");
	if (!version_id_val) {
		return "ViewHistoryEntry required property 'version-id' is missing";
	} else {
		if (yyjson_is_sint(version_id_val)) {
			version_id = yyjson_get_sint(version_id_val);
		} else {
			return "ViewHistoryEntry property 'version_id' is not of type 'integer'";
		}
	}
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "ViewHistoryEntry required property 'timestamp-ms' is missing";
	} else {
		if (yyjson_is_sint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else {
			return "ViewHistoryEntry property 'timestamp_ms' is not of type 'integer'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
