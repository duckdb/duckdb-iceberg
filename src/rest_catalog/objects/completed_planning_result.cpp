
#include "rest_catalog/objects/completed_planning_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CompletedPlanningResult::CompletedPlanningResult() {
}
CompletedPlanningResult::Object10::Object10() {
}

CompletedPlanningResult::Object10 CompletedPlanningResult::Object10::FromJSON(JSONValue obj) {
	Object10 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CompletedPlanningResult::Object10 CompletedPlanningResult::Object10::Copy() const {
	Object10 res;
	res.status = status.Copy();
	if (storage_credentials.has_value()) {
		res.storage_credentials.emplace();
		(*res.storage_credentials).reserve((*storage_credentials).size());
		for (auto &item : (*storage_credentials)) {
			(*res.storage_credentials).emplace_back(item.Copy());
		}
	}
	return res;
}

string CompletedPlanningResult::Object10::TryFromJSON(JSONValue obj) {
	string error;
	auto status_val = obj.GetMember("status");
	if (!status_val.IsValid()) {
		return "Object10 required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto storage_credentials_val = obj.GetMember("storage-credentials");
	if (storage_credentials_val.IsValid()) {
		vector<StorageCredential> storage_credentials_tmp;
		if (storage_credentials_val.IsArray()) {
			storage_credentials_val.IterateArray([&](JSONValue storage_credentials_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				StorageCredential storage_credentials_tmp_item;
				error = storage_credentials_tmp_item.TryFromJSON(storage_credentials_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				storage_credentials_tmp.emplace_back(std::move(storage_credentials_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "Object10 property 'storage_credentials_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(storage_credentials_val).c_str());
		}
		storage_credentials = std::move(storage_credentials_tmp);
	}
	return "";
}

void CompletedPlanningResult::Object10::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: status
	auto status_json = status.ToJSON(writer);
	obj.Add("status", status_json);

	// Serialize: storage-credentials
	if (storage_credentials.has_value()) {
		auto &storage_credentials_value = *storage_credentials;
		auto storage_credentials_json = writer.CreateArray();
		for (const auto &storage_credentials_json_item : storage_credentials_value) {
			auto storage_credentials_json_item_json = storage_credentials_json_item.ToJSON(writer);
			storage_credentials_json.Append(storage_credentials_json_item_json);
		}
		obj.Add("storage-credentials", storage_credentials_json);
	}
}

JSONMutableValue CompletedPlanningResult::Object10::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

CompletedPlanningResult CompletedPlanningResult::FromJSON(JSONValue obj) {
	CompletedPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CompletedPlanningResult CompletedPlanningResult::Copy() const {
	CompletedPlanningResult res;
	res.scan_tasks = scan_tasks.Copy();
	res.object_10 = object_10.Copy();
	return res;
}

string CompletedPlanningResult::TryFromJSON(JSONValue obj) {
	string error;
	error = scan_tasks.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_10.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

void CompletedPlanningResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: ScanTasks
	scan_tasks.PopulateJSON(writer, obj);

	// Serialize base class: Object10
	object_10.PopulateJSON(writer, obj);
}

JSONMutableValue CompletedPlanningResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
