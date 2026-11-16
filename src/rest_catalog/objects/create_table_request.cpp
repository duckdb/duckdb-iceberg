
#include "rest_catalog/objects/create_table_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CreateTableRequest::CreateTableRequest() {
}

CreateTableRequest CreateTableRequest::FromJSON(JSONValue obj) {
	CreateTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CreateTableRequest CreateTableRequest::Copy() const {
	CreateTableRequest res;
	res.name = name;
	res.schema = schema.Copy();
	if (location.has_value()) {
		res.location.emplace();
		(*res.location) = (*location);
	}
	if (partition_spec.has_value()) {
		res.partition_spec.emplace();
		(*res.partition_spec) = (*partition_spec).Copy();
	}
	if (write_order.has_value()) {
		res.write_order.emplace();
		(*res.write_order) = (*write_order).Copy();
	}
	if (stage_create.has_value()) {
		res.stage_create.emplace();
		(*res.stage_create) = (*stage_create);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string CreateTableRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "CreateTableRequest required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("CreateTableRequest property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	auto schema_val = obj.GetMember("schema");
	if (!schema_val.IsValid()) {
		return "CreateTableRequest required property 'schema' is missing";
	} else {
		error = schema.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto location_val = obj.GetMember("location");
	if (location_val.IsValid()) {
		string location_tmp;
		if (json_utils::IsString(location_val)) {
			location_tmp = json_utils::GetString(location_val);
		} else {
			return StringUtil::Format(
			    "CreateTableRequest property 'location_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(location_val).c_str());
		}
		location = std::move(location_tmp);
	}
	auto partition_spec_val = obj.GetMember("partition-spec");
	if (partition_spec_val.IsValid()) {
		PartitionSpec partition_spec_tmp;
		error = partition_spec_tmp.TryFromJSON(partition_spec_val);
		if (!error.empty()) {
			return error;
		}
		partition_spec = std::move(partition_spec_tmp);
	}
	auto write_order_val = obj.GetMember("write-order");
	if (write_order_val.IsValid()) {
		SortOrder write_order_tmp;
		error = write_order_tmp.TryFromJSON(write_order_val);
		if (!error.empty()) {
			return error;
		}
		write_order = std::move(write_order_tmp);
	}
	auto stage_create_val = obj.GetMember("stage-create");
	if (stage_create_val.IsValid()) {
		bool stage_create_tmp;
		if (json_utils::IsBoolean(stage_create_val)) {
			stage_create_tmp = json_utils::GetBoolean(stage_create_val);
		} else {
			return StringUtil::Format(
			    "CreateTableRequest property 'stage_create_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(stage_create_val).c_str());
		}
		stage_create = std::move(stage_create_tmp);
	}
	auto properties_val = obj.GetMember("properties");
	if (properties_val.IsValid()) {
		unordered_map<string, string> properties_tmp;
		if (properties_val.IsObject()) {
			properties_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format(
					    "CreateTableRequest property 'tmp' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "CreateTableRequest property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	return "";
}

void CreateTableRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);

	// Serialize: schema
	auto schema_json = schema.ToJSON(writer);
	obj.Add("schema", schema_json);

	// Serialize: location
	if (location.has_value()) {
		auto &location_value = *location;
		auto location_json = writer.CreateString(location_value);
		obj.Add("location", location_json);
	}

	// Serialize: partition-spec
	if (partition_spec.has_value()) {
		auto &partition_spec_value = *partition_spec;
		auto partition_spec_json = partition_spec_value.ToJSON(writer);
		obj.Add("partition-spec", partition_spec_json);
	}

	// Serialize: write-order
	if (write_order.has_value()) {
		auto &write_order_value = *write_order;
		auto write_order_json = write_order_value.ToJSON(writer);
		obj.Add("write-order", write_order_json);
	}

	// Serialize: stage-create
	if (stage_create.has_value()) {
		auto &stage_create_value = *stage_create;
		auto stage_create_json = writer.CreateBoolean(stage_create_value);
		obj.Add("stage-create", stage_create_json);
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		auto properties_json = writer.CreateObject();
		for (const auto &[properties_json_key, properties_json_value] : properties_value) {
			auto properties_json_value_json = writer.CreateString(properties_json_value);
			properties_json.Add(properties_json_key, properties_json_value_json);
		}
		obj.Add("properties", properties_json);
	}
}

JSONMutableValue CreateTableRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
