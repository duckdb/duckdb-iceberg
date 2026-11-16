
#include "rest_catalog/objects/snapshot_references.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SnapshotReferences::SnapshotReferences() {
}

SnapshotReferences SnapshotReferences::FromJSON(JSONValue obj) {
	SnapshotReferences res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SnapshotReferences SnapshotReferences::Copy() const {
	SnapshotReferences res;
	for (auto &entry : additional_properties) {
		res.additional_properties.emplace(entry.first, entry.second.Copy());
	}
	return res;
}

string SnapshotReferences::TryFromJSON(JSONValue obj) {
	string error;
	obj.IterateObject([&](const string &key_str, JSONValue val) {
		if (!error.empty()) {
			return;
		}
		SnapshotReference tmp;
		error = tmp.TryFromJSON(val);
		if (!error.empty()) {
			return;
		}
		additional_properties.emplace(key_str, std::move(tmp));
	});
	if (!error.empty()) {
		return error;
	}
	return "";
}

void SnapshotReferences::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize additional properties
	for (const auto &[key, value] : additional_properties) {
		auto value_json = value.ToJSON(writer);
		obj.Add(key, value_json);
	}
}

JSONMutableValue SnapshotReferences::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
