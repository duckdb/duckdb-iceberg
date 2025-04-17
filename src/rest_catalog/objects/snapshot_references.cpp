
#include "rest_catalog/objects/snapshot_references.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SnapshotReferences::SnapshotReferences() {
}

SnapshotReferences SnapshotReferences::FromJSON(yyjson_val *obj) {
	SnapshotReferences res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SnapshotReferences::TryFromJSON(yyjson_val *obj) {
	string error;
	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(obj, idx, max, key, val) {
		auto key_str = yyjson_get_str(key);
		SnapshotReference tmp;
		error = tmp.TryFromJSON(val);
		if (!error.empty()) {
			return error;
		}
		additional_properties.emplace(key_str, std::move(tmp));
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
