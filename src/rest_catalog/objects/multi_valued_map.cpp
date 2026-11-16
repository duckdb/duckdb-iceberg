
#include "rest_catalog/objects/multi_valued_map.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

MultiValuedMap::MultiValuedMap() {
}

MultiValuedMap MultiValuedMap::FromJSON(JSONValue obj) {
	MultiValuedMap res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

MultiValuedMap MultiValuedMap::Copy() const {
	MultiValuedMap res;
	for (auto &entry : additional_properties) {
		res.additional_properties.emplace(entry.first, entry.second);
	}
	return res;
}

string MultiValuedMap::TryFromJSON(JSONValue obj) {
	string error;
	obj.IterateObject([&](const string &key_str, JSONValue val) {
		if (!error.empty()) {
			return;
		}
		vector<string> tmp;
		if (val.IsArray()) {
			val.IterateArray([&](JSONValue tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				string tmp_item;
				if (json_utils::IsString(tmp_item_val)) {
					tmp_item = json_utils::GetString(tmp_item_val);
				} else {
					error = StringUtil::Format(
					    "MultiValuedMap property 'tmp_item' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(tmp_item_val).c_str());
					return;
				}
				tmp.emplace_back(std::move(tmp_item));
			});
			if (!error.empty()) {
				return;
			}
		} else {
			error = StringUtil::Format("MultiValuedMap property 'tmp' is not of type 'array', found %s instead",
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

void MultiValuedMap::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize additional properties
	for (const auto &[key, value] : additional_properties) {
		auto value_json = writer.CreateArray();
		for (const auto &value_json_item : value) {
			auto value_json_item_json = writer.CreateString(value_json_item);
			value_json.Append(value_json_item_json);
		}
		obj.Add(key, value_json);
	}
}

JSONMutableValue MultiValuedMap::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
