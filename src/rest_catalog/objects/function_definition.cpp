
#include "rest_catalog/objects/function_definition.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionDefinition::FunctionDefinition() {
}

FunctionDefinition FunctionDefinition::FromJSON(JSONValue obj) {
	FunctionDefinition res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionDefinition FunctionDefinition::Copy() const {
	FunctionDefinition res;
	res.definition_id = definition_id;
	res.parameters.reserve(parameters.size());
	for (auto &item : parameters) {
		res.parameters.emplace_back(item.Copy());
	}
	res.return_type = return_type ? make_uniq<FunctionDataType>(return_type->Copy()) : nullptr;
	res.versions.reserve(versions.size());
	for (auto &item : versions) {
		res.versions.emplace_back(item.Copy());
	}
	res.current_version_id = current_version_id;
	res.function_type = function_type;
	if (specific_name.has_value()) {
		res.specific_name.emplace();
		(*res.specific_name) = (*specific_name);
	}
	if (return_nullable.has_value()) {
		res.return_nullable.emplace();
		(*res.return_nullable) = (*return_nullable);
	}
	if (_doc.has_value()) {
		res._doc.emplace();
		(*res._doc) = (*_doc);
	}
	return res;
}

string FunctionDefinition::TryFromJSON(JSONValue obj) {
	string error;
	auto definition_id_val = obj.GetMember("definition-id");
	if (!definition_id_val.IsValid()) {
		return "FunctionDefinition required property 'definition-id' is missing";
	} else {
		if (json_utils::IsString(definition_id_val)) {
			definition_id = json_utils::GetString(definition_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'definition_id' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(definition_id_val).c_str());
		}
	}
	auto parameters_val = obj.GetMember("parameters");
	if (!parameters_val.IsValid()) {
		return "FunctionDefinition required property 'parameters' is missing";
	} else {
		if (parameters_val.IsArray()) {
			parameters_val.IterateArray([&](JSONValue parameters_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionParameter parameters_item;
				error = parameters_item.TryFromJSON(parameters_item_val);
				if (!error.empty()) {
					return;
				}
				parameters.emplace_back(std::move(parameters_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'parameters' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(parameters_val).c_str());
		}
	}
	auto return_type_val = obj.GetMember("return-type");
	if (!return_type_val.IsValid()) {
		return "FunctionDefinition required property 'return-type' is missing";
	} else {
		return_type = make_uniq<FunctionDataType>();
		error = return_type->TryFromJSON(return_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto versions_val = obj.GetMember("versions");
	if (!versions_val.IsValid()) {
		return "FunctionDefinition required property 'versions' is missing";
	} else {
		if (versions_val.IsArray()) {
			versions_val.IterateArray([&](JSONValue versions_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionDefinitionVersion versions_item;
				error = versions_item.TryFromJSON(versions_item_val);
				if (!error.empty()) {
					return;
				}
				versions.emplace_back(std::move(versions_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("FunctionDefinition property 'versions' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(versions_val).c_str());
		}
	}
	auto current_version_id_val = obj.GetMember("current-version-id");
	if (!current_version_id_val.IsValid()) {
		return "FunctionDefinition required property 'current-version-id' is missing";
	} else {
		if (json_utils::IsInteger(current_version_id_val)) {
			current_version_id = json_utils::GetSignedInteger(current_version_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'current_version_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(current_version_id_val).c_str());
		}
	}
	auto function_type_val = obj.GetMember("function-type");
	if (!function_type_val.IsValid()) {
		return "FunctionDefinition required property 'function-type' is missing";
	} else {
		if (json_utils::IsString(function_type_val)) {
			function_type = json_utils::GetString(function_type_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'function_type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(function_type_val).c_str());
		}
	}
	auto specific_name_val = obj.GetMember("specific-name");
	if (specific_name_val.IsValid()) {
		string specific_name_tmp;
		if (json_utils::IsString(specific_name_val)) {
			specific_name_tmp = json_utils::GetString(specific_name_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'specific_name_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(specific_name_val).c_str());
		}
		specific_name = std::move(specific_name_tmp);
	}
	auto return_nullable_val = obj.GetMember("return-nullable");
	if (return_nullable_val.IsValid()) {
		bool return_nullable_tmp;
		if (json_utils::IsBoolean(return_nullable_val)) {
			return_nullable_tmp = json_utils::GetBoolean(return_nullable_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'return_nullable_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(return_nullable_val).c_str());
		}
		return_nullable = std::move(return_nullable_tmp);
	}
	auto _doc_val = obj.GetMember("doc");
	if (_doc_val.IsValid()) {
		string _doc_tmp;
		if (json_utils::IsString(_doc_val)) {
			_doc_tmp = json_utils::GetString(_doc_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property '_doc_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(_doc_val).c_str());
		}
		_doc = std::move(_doc_tmp);
	}
	return "";
}

void FunctionDefinition::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: definition-id
	auto definition_id_json = writer.CreateString(definition_id);
	obj.Add("definition-id", definition_id_json);

	// Serialize: parameters
	auto parameters_json = writer.CreateArray();
	for (const auto &parameters_json_item : parameters) {
		auto parameters_json_item_json = parameters_json_item.ToJSON(writer);
		parameters_json.Append(parameters_json_item_json);
	}
	obj.Add("parameters", parameters_json);

	// Serialize: return-type
	auto return_type_json = return_type->ToJSON(writer);
	obj.Add("return-type", return_type_json);

	// Serialize: versions
	auto versions_json = writer.CreateArray();
	for (const auto &versions_json_item : versions) {
		auto versions_json_item_json = versions_json_item.ToJSON(writer);
		versions_json.Append(versions_json_item_json);
	}
	obj.Add("versions", versions_json);

	// Serialize: current-version-id
	auto current_version_id_json = writer.CreateSignedInteger(current_version_id);
	obj.Add("current-version-id", current_version_id_json);

	// Serialize: function-type
	auto function_type_json = writer.CreateString(function_type);
	obj.Add("function-type", function_type_json);

	// Serialize: specific-name
	if (specific_name.has_value()) {
		auto &specific_name_value = *specific_name;
		auto specific_name_json = writer.CreateString(specific_name_value);
		obj.Add("specific-name", specific_name_json);
	}

	// Serialize: return-nullable
	if (return_nullable.has_value()) {
		auto &return_nullable_value = *return_nullable;
		auto return_nullable_json = writer.CreateBoolean(return_nullable_value);
		obj.Add("return-nullable", return_nullable_json);
	}

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		auto _doc_json = writer.CreateString(_doc_value);
		obj.Add("doc", _doc_json);
	}
}

JSONMutableValue FunctionDefinition::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
