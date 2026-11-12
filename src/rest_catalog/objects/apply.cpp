
#include "rest_catalog/objects/apply.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Apply::Apply() {
}
Apply::Object4::Object4() {
}
Apply::Object4::InlineSchema2::InlineSchema2() {
}

Apply::Object4::InlineSchema2 Apply::Object4::InlineSchema2::FromJSON(JSONValue obj) {
	InlineSchema2 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Apply::Object4::InlineSchema2 Apply::Object4::InlineSchema2::Copy() const {
	InlineSchema2 res;
	res.value = value;
	return res;
}

string Apply::Object4::InlineSchema2::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsString(obj)) {
		value = json_utils::GetString(obj);
	} else {
		return StringUtil::Format("InlineSchema2 property 'value' is not of type 'string', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue Apply::Object4::InlineSchema2::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateString(value);
	return result;
}
Apply::Object4::Object3::Object3() {
}

Apply::Object4::Object3 Apply::Object4::Object3::FromJSON(JSONValue obj) {
	Object3 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Apply::Object4::Object3 Apply::Object4::Object3::Copy() const {
	Object3 res;
	res.identifier = identifier.Copy();
	if (catalog.has_value()) {
		res.catalog.emplace();
		(*res.catalog) = (*catalog);
	}
	return res;
}

string Apply::Object4::Object3::TryFromJSON(JSONValue obj) {
	string error;
	auto identifier_val = obj.GetMember("identifier");
	if (!identifier_val.IsValid()) {
		return "Object3 required property 'identifier' is missing";
	} else {
		error = identifier.TryFromJSON(identifier_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto catalog_val = obj.GetMember("catalog");
	if (catalog_val.IsValid()) {
		string catalog_tmp;
		if (json_utils::IsString(catalog_val)) {
			catalog_tmp = json_utils::GetString(catalog_val);
		} else {
			return StringUtil::Format("Object3 property 'catalog_tmp' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(catalog_val).c_str());
		}
		catalog = std::move(catalog_tmp);
	}
	return "";
}

void Apply::Object4::Object3::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: identifier
	auto identifier_json = identifier.ToJSON(writer);
	obj.Add("identifier", identifier_json);

	// Serialize: catalog
	if (catalog.has_value()) {
		auto &catalog_value = *catalog;
		auto catalog_json = writer.CreateString(catalog_value);
		obj.Add("catalog", catalog_json);
	}
}

JSONMutableValue Apply::Object4::Object3::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

Apply::Object4 Apply::Object4::FromJSON(JSONValue obj) {
	Object4 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Apply::Object4 Apply::Object4::Copy() const {
	Object4 res;
	if (inline_schema_2.has_value()) {
		res.inline_schema_2.emplace();
		(*res.inline_schema_2) = (*inline_schema_2).Copy();
	}
	if (catalog_object_identifier.has_value()) {
		res.catalog_object_identifier.emplace();
		(*res.catalog_object_identifier) = (*catalog_object_identifier).Copy();
	}
	if (object_3.has_value()) {
		res.object_3.emplace();
		(*res.object_3) = (*object_3).Copy();
	}
	return res;
}

string Apply::Object4::TryFromJSON(JSONValue obj) {
	string error;
	do {
		inline_schema_2.emplace();
		error = inline_schema_2->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			inline_schema_2 = nullopt;
		}
		catalog_object_identifier.emplace();
		error = catalog_object_identifier->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			catalog_object_identifier = nullopt;
		}
		object_3.emplace();
		error = object_3->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			object_3 = nullopt;
		}
		return "Object4 failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

JSONMutableValue Apply::Object4::ToJSON(JSONWriter &writer) const {
	if (inline_schema_2.has_value()) {
		return inline_schema_2->ToJSON(writer);
	} else if (catalog_object_identifier.has_value()) {
		return catalog_object_identifier->ToJSON(writer);
	} else if (object_3.has_value()) {
		return object_3->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

Apply Apply::FromJSON(JSONValue obj) {
	Apply res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Apply Apply::Copy() const {
	Apply res;
	res.type = type;
	res.function = function.Copy();
	res.arguments.reserve(arguments.size());
	for (auto &item : arguments) {
		res.arguments.emplace_back(item.Copy());
	}
	return res;
}

string Apply::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "Apply required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("Apply property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "apply") {
			return "Apply property 'type' does not match its required const value";
		}
	}
	auto function_val = obj.GetMember("function");
	if (!function_val.IsValid()) {
		return "Apply required property 'function' is missing";
	} else {
		error = function.TryFromJSON(function_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto arguments_val = obj.GetMember("arguments");
	if (!arguments_val.IsValid()) {
		return "Apply required property 'arguments' is missing";
	} else {
		if (arguments_val.IsArray()) {
			arguments_val.IterateArray([&](JSONValue arguments_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionArgument arguments_item;
				error = arguments_item.TryFromJSON(arguments_item_val);
				if (!error.empty()) {
					return;
				}
				arguments.emplace_back(std::move(arguments_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("Apply property 'arguments' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(arguments_val).c_str());
		}
	}
	return "";
}

void Apply::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: function
	auto function_json = function.ToJSON(writer);
	obj.Add("function", function_json);

	// Serialize: arguments
	auto arguments_json = writer.CreateArray();
	for (const auto &arguments_json_item : arguments) {
		auto arguments_json_item_json = arguments_json_item.ToJSON(writer);
		arguments_json.Append(arguments_json_item_json);
	}
	obj.Add("arguments", arguments_json);
}

JSONMutableValue Apply::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
