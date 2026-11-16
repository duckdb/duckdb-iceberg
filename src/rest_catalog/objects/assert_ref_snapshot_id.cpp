
#include "rest_catalog/objects/assert_ref_snapshot_id.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AssertRefSnapshotId::AssertRefSnapshotId() {
}

AssertRefSnapshotId AssertRefSnapshotId::FromJSON(JSONValue obj) {
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

string AssertRefSnapshotId::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AssertRefSnapshotId required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("AssertRefSnapshotId property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "assert-ref-snapshot-id") {
			return "AssertRefSnapshotId property 'type' does not match its required const value";
		}
	}
	auto ref_val = obj.GetMember("ref");
	if (!ref_val.IsValid()) {
		return "AssertRefSnapshotId required property 'ref' is missing";
	} else {
		if (json_utils::IsString(ref_val)) {
			ref = json_utils::GetString(ref_val);
		} else {
			return StringUtil::Format("AssertRefSnapshotId property 'ref' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(ref_val).c_str());
		}
	}
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "AssertRefSnapshotId required property 'snapshot-id' is missing";
	} else {
		if (snapshot_id_val.IsNull()) {
			snapshot_id = nullopt;
		} else {
			int64_t snapshot_id_tmp;
			if (json_utils::IsInteger(snapshot_id_val)) {
				snapshot_id_tmp = json_utils::GetSignedInteger(snapshot_id_val);
			} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
				snapshot_id_tmp = json_utils::GetUnsignedInteger(snapshot_id_val);
			} else {
				return StringUtil::Format(
				    "AssertRefSnapshotId property 'snapshot_id_tmp' is not of type 'integer', found %s instead",
				    json_utils::GetTypeDescription(snapshot_id_val).c_str());
			}
			snapshot_id = std::move(snapshot_id_tmp);
		}
	}
	return "";
}

void AssertRefSnapshotId::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: ref
	auto ref_json = writer.CreateString(ref);
	obj.Add("ref", ref_json);

	// Serialize: snapshot-id
	if (snapshot_id.has_value()) {
		auto &snapshot_id_value = *snapshot_id;
		auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id_value);
		obj.Add("snapshot-id", snapshot_id_json);
	} else {
		obj.Add("snapshot-id", writer.CreateNull());
	}
}

JSONMutableValue AssertRefSnapshotId::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
