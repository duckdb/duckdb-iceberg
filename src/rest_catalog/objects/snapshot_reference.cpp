
#include "rest_catalog/objects/snapshot_reference.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SnapshotReference::SnapshotReference() {
}

SnapshotReference SnapshotReference::FromJSON(JSONValue obj) {
	SnapshotReference res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SnapshotReference SnapshotReference::Copy() const {
	SnapshotReference res;
	res.type = type;
	res.snapshot_id = snapshot_id;
	if (max_ref_age_ms.has_value()) {
		res.max_ref_age_ms.emplace();
		(*res.max_ref_age_ms) = (*max_ref_age_ms);
	}
	if (max_snapshot_age_ms.has_value()) {
		res.max_snapshot_age_ms.emplace();
		(*res.max_snapshot_age_ms) = (*max_snapshot_age_ms);
	}
	if (min_snapshots_to_keep.has_value()) {
		res.min_snapshots_to_keep.emplace();
		(*res.min_snapshots_to_keep) = (*min_snapshots_to_keep);
	}
	return res;
}

string SnapshotReference::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "SnapshotReference required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("SnapshotReference property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
	}
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "SnapshotReference required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'snapshot_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto max_ref_age_ms_val = obj.GetMember("max-ref-age-ms");
	if (max_ref_age_ms_val.IsValid()) {
		int64_t max_ref_age_ms_tmp;
		if (json_utils::IsInteger(max_ref_age_ms_val)) {
			max_ref_age_ms_tmp = json_utils::GetSignedInteger(max_ref_age_ms_val);
		} else if (json_utils::IsUnsignedInteger(max_ref_age_ms_val)) {
			max_ref_age_ms_tmp = json_utils::GetUnsignedInteger(max_ref_age_ms_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'max_ref_age_ms_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(max_ref_age_ms_val).c_str());
		}
		max_ref_age_ms = std::move(max_ref_age_ms_tmp);
	}
	auto max_snapshot_age_ms_val = obj.GetMember("max-snapshot-age-ms");
	if (max_snapshot_age_ms_val.IsValid()) {
		int64_t max_snapshot_age_ms_tmp;
		if (json_utils::IsInteger(max_snapshot_age_ms_val)) {
			max_snapshot_age_ms_tmp = json_utils::GetSignedInteger(max_snapshot_age_ms_val);
		} else if (json_utils::IsUnsignedInteger(max_snapshot_age_ms_val)) {
			max_snapshot_age_ms_tmp = json_utils::GetUnsignedInteger(max_snapshot_age_ms_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'max_snapshot_age_ms_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(max_snapshot_age_ms_val).c_str());
		}
		max_snapshot_age_ms = std::move(max_snapshot_age_ms_tmp);
	}
	auto min_snapshots_to_keep_val = obj.GetMember("min-snapshots-to-keep");
	if (min_snapshots_to_keep_val.IsValid()) {
		int32_t min_snapshots_to_keep_tmp;
		if (json_utils::IsInteger(min_snapshots_to_keep_val)) {
			min_snapshots_to_keep_tmp = json_utils::GetSignedInteger(min_snapshots_to_keep_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'min_snapshots_to_keep_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(min_snapshots_to_keep_val).c_str());
		}
		min_snapshots_to_keep = std::move(min_snapshots_to_keep_tmp);
	}
	return "";
}

void SnapshotReference::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: snapshot-id
	auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id);
	obj.Add("snapshot-id", snapshot_id_json);

	// Serialize: max-ref-age-ms
	if (max_ref_age_ms.has_value()) {
		auto &max_ref_age_ms_value = *max_ref_age_ms;
		auto max_ref_age_ms_json = writer.CreateSignedInteger(max_ref_age_ms_value);
		obj.Add("max-ref-age-ms", max_ref_age_ms_json);
	}

	// Serialize: max-snapshot-age-ms
	if (max_snapshot_age_ms.has_value()) {
		auto &max_snapshot_age_ms_value = *max_snapshot_age_ms;
		auto max_snapshot_age_ms_json = writer.CreateSignedInteger(max_snapshot_age_ms_value);
		obj.Add("max-snapshot-age-ms", max_snapshot_age_ms_json);
	}

	// Serialize: min-snapshots-to-keep
	if (min_snapshots_to_keep.has_value()) {
		auto &min_snapshots_to_keep_value = *min_snapshots_to_keep;
		auto min_snapshots_to_keep_json = writer.CreateSignedInteger(min_snapshots_to_keep_value);
		obj.Add("min-snapshots-to-keep", min_snapshots_to_keep_json);
	}
}

JSONMutableValue SnapshotReference::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
