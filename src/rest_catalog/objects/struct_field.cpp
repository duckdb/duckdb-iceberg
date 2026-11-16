
#include "rest_catalog/objects/struct_field.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

StructField::StructField() {
}

StructField StructField::FromJSON(JSONValue obj) {
	StructField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

StructField StructField::Copy() const {
	StructField res;
	res.id = id;
	res.name = name;
	res.type = type ? make_uniq<Type>(type->Copy()) : nullptr;
	res.required = required;
	if (_doc.has_value()) {
		res._doc.emplace();
		(*res._doc) = (*_doc);
	}
	if (initial_default.has_value()) {
		res.initial_default.emplace();
		(*res.initial_default) = (*initial_default).Copy();
	}
	if (write_default.has_value()) {
		res.write_default.emplace();
		(*res.write_default) = (*write_default).Copy();
	}
	return res;
}

string StructField::TryFromJSON(JSONValue obj) {
	string error;
	auto id_val = obj.GetMember("id");
	if (!id_val.IsValid()) {
		return "StructField required property 'id' is missing";
	} else {
		if (json_utils::IsInteger(id_val)) {
			id = json_utils::GetSignedInteger(id_val);
		} else {
			return StringUtil::Format("StructField property 'id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(id_val).c_str());
		}
	}
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "StructField required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("StructField property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "StructField required property 'type' is missing";
	} else {
		type = make_uniq<Type>();
		error = type->TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto required_val = obj.GetMember("required");
	if (!required_val.IsValid()) {
		return "StructField required property 'required' is missing";
	} else {
		if (json_utils::IsBoolean(required_val)) {
			required = json_utils::GetBoolean(required_val);
		} else {
			return StringUtil::Format("StructField property 'required' is not of type 'boolean', found %s instead",
			                          json_utils::GetTypeDescription(required_val).c_str());
		}
	}
	auto _doc_val = obj.GetMember("doc");
	if (_doc_val.IsValid()) {
		string _doc_tmp;
		if (json_utils::IsString(_doc_val)) {
			_doc_tmp = json_utils::GetString(_doc_val);
		} else {
			return StringUtil::Format("StructField property '_doc_tmp' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(_doc_val).c_str());
		}
		_doc = std::move(_doc_tmp);
	}
	auto initial_default_val = obj.GetMember("initial-default");
	if (initial_default_val.IsValid()) {
		PrimitiveTypeValue initial_default_tmp;
		error = initial_default_tmp.TryFromJSON(initial_default_val);
		if (!error.empty()) {
			return error;
		}
		initial_default = std::move(initial_default_tmp);
	}
	auto write_default_val = obj.GetMember("write-default");
	if (write_default_val.IsValid()) {
		PrimitiveTypeValue write_default_tmp;
		error = write_default_tmp.TryFromJSON(write_default_val);
		if (!error.empty()) {
			return error;
		}
		write_default = std::move(write_default_tmp);
	}
	return "";
}

void StructField::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: id
	auto id_json = writer.CreateSignedInteger(id);
	obj.Add("id", id_json);

	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);

	// Serialize: type
	auto type_json = type->ToJSON(writer);
	obj.Add("type", type_json);

	// Serialize: required
	auto required_json = writer.CreateBoolean(required);
	obj.Add("required", required_json);

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		auto _doc_json = writer.CreateString(_doc_value);
		obj.Add("doc", _doc_json);
	}

	// Serialize: initial-default
	if (initial_default.has_value()) {
		auto &initial_default_value = *initial_default;
		auto initial_default_json = initial_default_value.ToJSON(writer);
		obj.Add("initial-default", initial_default_json);
	}

	// Serialize: write-default
	if (write_default.has_value()) {
		auto &write_default_value = *write_default;
		auto write_default_json = write_default_value.ToJSON(writer);
		obj.Add("write-default", write_default_json);
	}
}

JSONMutableValue StructField::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
