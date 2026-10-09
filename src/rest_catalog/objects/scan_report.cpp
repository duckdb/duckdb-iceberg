
#include "rest_catalog/objects/scan_report.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ScanReport::ScanReport() {
}

ScanReport ScanReport::FromJSON(JSONValue obj) {
	ScanReport res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ScanReport ScanReport::Copy() const {
	ScanReport res;
	res.table_name = table_name;
	res.snapshot_id = snapshot_id;
	res.filter = filter ? make_uniq<Predicate>(filter->Copy()) : nullptr;
	res.schema_id = schema_id;
	res.projected_field_ids.reserve(projected_field_ids.size());
	for (auto &item : projected_field_ids) {
		res.projected_field_ids.emplace_back(item);
	}
	res.projected_field_names.reserve(projected_field_names.size());
	for (auto &item : projected_field_names) {
		res.projected_field_names.emplace_back(item);
	}
	res.metrics = metrics.Copy();
	if (metadata.has_value()) {
		res.metadata.emplace();
		for (auto &entry : (*metadata)) {
			(*res.metadata).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string ScanReport::TryFromJSON(JSONValue obj) {
	string error;
	auto table_name_val = obj.GetMember("table-name");
	if (!table_name_val.IsValid()) {
		return "ScanReport required property 'table-name' is missing";
	} else {
		if (json_utils::IsString(table_name_val)) {
			table_name = json_utils::GetString(table_name_val);
		} else {
			return StringUtil::Format("ScanReport property 'table_name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(table_name_val).c_str());
		}
	}
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "ScanReport required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format("ScanReport property 'snapshot_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto filter_val = obj.GetMember("filter");
	if (!filter_val.IsValid()) {
		return "ScanReport required property 'filter' is missing";
	} else {
		filter = make_uniq<Predicate>();
		error = filter->TryFromJSON(filter_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto schema_id_val = obj.GetMember("schema-id");
	if (!schema_id_val.IsValid()) {
		return "ScanReport required property 'schema-id' is missing";
	} else {
		if (json_utils::IsInteger(schema_id_val)) {
			schema_id = json_utils::GetSignedInteger(schema_id_val);
		} else {
			return StringUtil::Format("ScanReport property 'schema_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(schema_id_val).c_str());
		}
	}
	auto projected_field_ids_val = obj.GetMember("projected-field-ids");
	if (!projected_field_ids_val.IsValid()) {
		return "ScanReport required property 'projected-field-ids' is missing";
	} else {
		if (projected_field_ids_val.IsArray()) {
			projected_field_ids_val.IterateArray([&](JSONValue projected_field_ids_item_val) {
				if (!error.empty()) {
					return;
				}
				int32_t projected_field_ids_item;
				if (json_utils::IsInteger(projected_field_ids_item_val)) {
					projected_field_ids_item = json_utils::GetSignedInteger(projected_field_ids_item_val);
				} else {
					error = StringUtil::Format(
					    "ScanReport property 'projected_field_ids_item' is not of type 'integer', found %s instead",
					    json_utils::GetTypeDescription(projected_field_ids_item_val).c_str());
					return;
				}
				projected_field_ids.emplace_back(std::move(projected_field_ids_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ScanReport property 'projected_field_ids' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(projected_field_ids_val).c_str());
		}
	}
	auto projected_field_names_val = obj.GetMember("projected-field-names");
	if (!projected_field_names_val.IsValid()) {
		return "ScanReport required property 'projected-field-names' is missing";
	} else {
		if (projected_field_names_val.IsArray()) {
			projected_field_names_val.IterateArray([&](JSONValue projected_field_names_item_val) {
				if (!error.empty()) {
					return;
				}
				string projected_field_names_item;
				if (json_utils::IsString(projected_field_names_item_val)) {
					projected_field_names_item = json_utils::GetString(projected_field_names_item_val);
				} else {
					error = StringUtil::Format(
					    "ScanReport property 'projected_field_names_item' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(projected_field_names_item_val).c_str());
					return;
				}
				projected_field_names.emplace_back(std::move(projected_field_names_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ScanReport property 'projected_field_names' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(projected_field_names_val).c_str());
		}
	}
	auto metrics_val = obj.GetMember("metrics");
	if (!metrics_val.IsValid()) {
		return "ScanReport required property 'metrics' is missing";
	} else {
		error = metrics.TryFromJSON(metrics_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_val = obj.GetMember("metadata");
	if (metadata_val.IsValid()) {
		case_insensitive_map_t<string> metadata_tmp;
		if (metadata_val.IsObject()) {
			metadata_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("ScanReport property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				metadata_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "ScanReport property 'metadata_tmp' is not of type 'object'";
		}
		metadata = std::move(metadata_tmp);
	}
	return "";
}

void ScanReport::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: table-name
	auto table_name_json = writer.CreateString(table_name);
	obj.Add("table-name", table_name_json);

	// Serialize: snapshot-id
	auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id);
	obj.Add("snapshot-id", snapshot_id_json);

	// Serialize: filter
	auto filter_json = filter->ToJSON(writer);
	obj.Add("filter", filter_json);

	// Serialize: schema-id
	auto schema_id_json = writer.CreateSignedInteger(schema_id);
	obj.Add("schema-id", schema_id_json);

	// Serialize: projected-field-ids
	auto projected_field_ids_json = writer.CreateArray();
	for (const auto &projected_field_ids_json_item : projected_field_ids) {
		auto projected_field_ids_json_item_json = writer.CreateSignedInteger(projected_field_ids_json_item);
		projected_field_ids_json.Append(projected_field_ids_json_item_json);
	}
	obj.Add("projected-field-ids", projected_field_ids_json);

	// Serialize: projected-field-names
	auto projected_field_names_json = writer.CreateArray();
	for (const auto &projected_field_names_json_item : projected_field_names) {
		auto projected_field_names_json_item_json = writer.CreateString(projected_field_names_json_item);
		projected_field_names_json.Append(projected_field_names_json_item_json);
	}
	obj.Add("projected-field-names", projected_field_names_json);

	// Serialize: metrics
	auto metrics_json = metrics.ToJSON(writer);
	obj.Add("metrics", metrics_json);

	// Serialize: metadata
	if (metadata.has_value()) {
		auto &metadata_value = *metadata;
		auto metadata_json = writer.CreateObject();
		for (const auto &[metadata_json_key, metadata_json_value] : metadata_value) {
			auto metadata_json_value_json = writer.CreateString(metadata_json_value);
			metadata_json.Add(metadata_json_key, metadata_json_value_json);
		}
		obj.Add("metadata", metadata_json);
	}
}

JSONMutableValue ScanReport::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
