
#include "rest_catalog/objects/namespace.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Namespace::Namespace() {
}

Namespace Namespace::FromJSON(JSONValue obj) {
	Namespace res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Namespace Namespace::Copy() const {
	Namespace res;
	res.value.reserve(value.size());
	for (auto &item : value) {
		res.value.emplace_back(item);
	}
	return res;
}

string Namespace::TryFromJSON(JSONValue obj) {
	string error;
	if (obj.IsArray()) {
		obj.IterateArray([&](JSONValue value_item_val) {
			if (!error.empty()) {
				return;
			}
			string value_item;
			if (json_utils::IsString(value_item_val)) {
				value_item = json_utils::GetString(value_item_val);
			} else {
				error = StringUtil::Format("Namespace property 'value_item' is not of type 'string', found %s instead",
				                           json_utils::GetTypeDescription(value_item_val).c_str());
				return;
			}
			value.emplace_back(std::move(value_item));
		});
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("Namespace property 'value' is not of type 'array', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue Namespace::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateArray();
	for (const auto &result_item : value) {
		auto result_item_json = writer.CreateString(result_item);
		result.Append(result_item_json);
	}
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
