
#include "rest_catalog/objects/assert_ref_snapshot_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertRefSnapshotId::AssertRefSnapshotId() {
}

AssertRefSnapshotId AssertRefSnapshotId::FromJSON(yyjson_val *obj) {
	AssertRefSnapshotId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertRefSnapshotId AssertRefSnapshotId::Copy() const {
	AssertRefSnapshotId res;
	res.type = type;
	res.ref = ref;
	if (snapshot_id.has_value()) {
		res.snapshot_id.emplace();
		(*res.snapshot_id) = (*snapshot_id);
	}
	return res;
}

string AssertRefSnapshotId::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertRefSnapshotId required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("AssertRefSnapshotId property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "assert-ref-snapshot-id") {
			return "AssertRefSnapshotId property 'type' does not match its required const value";
		}
	}
	auto ref_val = yyjson_obj_get(obj, "ref");
	if (!ref_val) {
		return "AssertRefSnapshotId required property 'ref' is missing";
	} else {
		if (yyjson_is_str(ref_val)) {
			ref = yyjson_get_str(ref_val);
		} else {
			return StringUtil::Format("AssertRefSnapshotId property 'ref' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(ref_val));
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "AssertRefSnapshotId required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_null(snapshot_id_val)) {
			snapshot_id = nullopt;
		} else {
			int64_t snapshot_id_tmp;
			if (yyjson_is_sint(snapshot_id_val)) {
				snapshot_id_tmp = yyjson_get_sint(snapshot_id_val);
			} else if (yyjson_is_uint(snapshot_id_val)) {
				snapshot_id_tmp = yyjson_get_uint(snapshot_id_val);
			} else {
				return StringUtil::Format(
				    "AssertRefSnapshotId property 'snapshot_id_tmp' is not of type 'integer', found '%s' instead",
				    yyjson_get_type_desc(snapshot_id_val));
			}
			snapshot_id = std::move(snapshot_id_tmp);
		}
	}
	return "";
}

void AssertRefSnapshotId::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: ref
	yyjson_mut_obj_add_strcpy(doc, obj, "ref", ref.c_str());

	// Serialize: snapshot-id
	if (snapshot_id.has_value()) {
		auto &snapshot_id_value = *snapshot_id;
		yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id_value);
	} else {
		yyjson_mut_obj_add_null(doc, obj, "snapshot-id");
	}
}

yyjson_mut_val *AssertRefSnapshotId::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
