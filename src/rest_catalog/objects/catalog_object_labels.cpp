
#include "rest_catalog/objects/catalog_object_labels.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CatalogObjectLabels::CatalogObjectLabels() {
}

CatalogObjectLabels CatalogObjectLabels::FromJSON(JSONValue obj) {
	CatalogObjectLabels res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CatalogObjectLabels CatalogObjectLabels::Copy() const {
	CatalogObjectLabels res;
	for (auto &entry : additional_properties) {
		res.additional_properties.emplace(entry.first, entry.second);
	}
	return res;
}

string CatalogObjectLabels::TryFromJSON(JSONValue obj) {
	string error;
	obj.IterateObject([&](const string &key_str, JSONValue val) {
		if (!error.empty()) {
			return;
		}
		string tmp;
		if (json_utils::IsString(val)) {
			tmp = json_utils::GetString(val);
		} else {
			error = StringUtil::Format("CatalogObjectLabels property 'tmp' is not of type 'string', found %s instead",
			                           json_utils::GetTypeDescription(val).c_str());
			return;
		}
		additional_properties.emplace(key_str, std::move(tmp));
	});
	if (!error.empty()) {
		return error;
	}
	return "";
}

void CatalogObjectLabels::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize additional properties
	for (const auto &[key, value] : additional_properties) {
		auto value_json = writer.CreateString(value);
		obj.Add(key, value_json);
	}
}

JSONMutableValue CatalogObjectLabels::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
