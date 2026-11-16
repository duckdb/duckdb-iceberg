
#include "rest_catalog/objects/value_map.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ValueMap::ValueMap() {
}

ValueMap ValueMap::FromJSON(JSONValue obj) {
	ValueMap res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ValueMap ValueMap::Copy() const {
	ValueMap res;
	if (keys.has_value()) {
		res.keys.emplace();
		(*res.keys).reserve((*keys).size());
		for (auto &item : (*keys)) {
			(*res.keys).emplace_back(item.Copy());
		}
	}
	if (values.has_value()) {
		res.values.emplace();
		(*res.values).reserve((*values).size());
		for (auto &item : (*values)) {
			(*res.values).emplace_back(item.Copy());
		}
	}
	return res;
}

string ValueMap::TryFromJSON(JSONValue obj) {
	string error;
	auto keys_val = obj.GetMember("keys");
	if (keys_val.IsValid()) {
		vector<IntegerTypeValue> keys_tmp;
		if (keys_val.IsArray()) {
			keys_val.IterateArray([&](JSONValue keys_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				IntegerTypeValue keys_tmp_item;
				error = keys_tmp_item.TryFromJSON(keys_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				keys_tmp.emplace_back(std::move(keys_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ValueMap property 'keys_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(keys_val).c_str());
		}
		keys = std::move(keys_tmp);
	}
	auto values_val = obj.GetMember("values");
	if (values_val.IsValid()) {
		vector<PrimitiveTypeValue> values_tmp;
		if (values_val.IsArray()) {
			values_val.IterateArray([&](JSONValue values_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				PrimitiveTypeValue values_tmp_item;
				error = values_tmp_item.TryFromJSON(values_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				values_tmp.emplace_back(std::move(values_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ValueMap property 'values_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(values_val).c_str());
		}
		values = std::move(values_tmp);
	}
	return "";
}

void ValueMap::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: keys
	if (keys.has_value()) {
		auto &keys_value = *keys;
		auto keys_json = writer.CreateArray();
		for (const auto &keys_json_item : keys_value) {
			auto keys_json_item_json = keys_json_item.ToJSON(writer);
			keys_json.Append(keys_json_item_json);
		}
		obj.Add("keys", keys_json);
	}

	// Serialize: values
	if (values.has_value()) {
		auto &values_value = *values;
		auto values_json = writer.CreateArray();
		for (const auto &values_json_item : values_value) {
			auto values_json_item_json = values_json_item.ToJSON(writer);
			values_json.Append(values_json_item_json);
		}
		obj.Add("values", values_json);
	}
}

JSONMutableValue ValueMap::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
