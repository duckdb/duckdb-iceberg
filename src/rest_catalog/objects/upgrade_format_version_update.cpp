
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

UpgradeFormatVersionUpdate::UpgradeFormatVersionUpdate() {
}

UpgradeFormatVersionUpdate UpgradeFormatVersionUpdate::FromJSON(JSONValue obj) {
	UpgradeFormatVersionUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UpgradeFormatVersionUpdate UpgradeFormatVersionUpdate::Copy() const {
	UpgradeFormatVersionUpdate res;
	res.base_update = base_update.Copy();
	res.format_version = format_version;
	return res;
}

string UpgradeFormatVersionUpdate::TryFromJSON(JSONValue obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = obj.GetMember("action");
	if (action_refinement_val.IsValid()) {
		string action_refinement;
		if (json_utils::IsString(action_refinement_val)) {
			action_refinement = json_utils::GetString(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "UpgradeFormatVersionUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "upgrade-format-version") {
			return "UpgradeFormatVersionUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "UpgradeFormatVersionUpdate required property 'action' is missing";
	}
	auto format_version_val = obj.GetMember("format-version");
	if (!format_version_val.IsValid()) {
		return "UpgradeFormatVersionUpdate required property 'format-version' is missing";
	} else {
		if (json_utils::IsInteger(format_version_val)) {
			format_version = json_utils::GetSignedInteger(format_version_val);
		} else {
			return StringUtil::Format(
			    "UpgradeFormatVersionUpdate property 'format_version' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(format_version_val).c_str());
		}
	}
	return "";
}

void UpgradeFormatVersionUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: format-version
	auto format_version_json = writer.CreateSignedInteger(format_version);
	obj.Add("format-version", format_version_json);
}

JSONMutableValue UpgradeFormatVersionUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
