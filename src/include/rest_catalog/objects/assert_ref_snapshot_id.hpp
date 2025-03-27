#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertRefSnapshotId {
public:
	static AssertRefSnapshotId FromJSON(yyjson_val *obj) {
		AssertRefSnapshotId result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto ref_val = yyjson_obj_get(obj, "ref");
		if (ref_val) {
			result.ref = yyjson_get_str(ref_val);
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		return result;
	}
public:
	string type;
	string ref;
	int64_t snapshot_id;
};

} // namespace rest_api_objects
} // namespace duckdb