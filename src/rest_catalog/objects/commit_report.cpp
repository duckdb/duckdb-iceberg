
#include "rest_catalog/objects/commit_report.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CommitReport::CommitReport() {
}

CommitReport CommitReport::FromJSON(JSONValue obj) {
	CommitReport res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CommitReport CommitReport::Copy() const {
	CommitReport res;
	res.table_name = table_name;
	res.snapshot_id = snapshot_id;
	res.sequence_number = sequence_number;
	res.operation = operation;
	res.metrics = metrics.Copy();
	if (metadata.has_value()) {
		res.metadata.emplace();
		for (auto &entry : (*metadata)) {
			(*res.metadata).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string CommitReport::TryFromJSON(JSONValue obj) {
	string error;
	auto table_name_val = obj.GetMember("table-name");
	if (!table_name_val.IsValid()) {
		return "CommitReport required property 'table-name' is missing";
	} else {
		if (json_utils::IsString(table_name_val)) {
			table_name = json_utils::GetString(table_name_val);
		} else {
			return StringUtil::Format("CommitReport property 'table_name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(table_name_val).c_str());
		}
	}
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (!snapshot_id_val.IsValid()) {
		return "CommitReport required property 'snapshot-id' is missing";
	} else {
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format("CommitReport property 'snapshot_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
	}
	auto sequence_number_val = obj.GetMember("sequence-number");
	if (!sequence_number_val.IsValid()) {
		return "CommitReport required property 'sequence-number' is missing";
	} else {
		if (json_utils::IsInteger(sequence_number_val)) {
			sequence_number = json_utils::GetSignedInteger(sequence_number_val);
		} else if (json_utils::IsUnsignedInteger(sequence_number_val)) {
			sequence_number = json_utils::GetUnsignedInteger(sequence_number_val);
		} else {
			return StringUtil::Format(
			    "CommitReport property 'sequence_number' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(sequence_number_val).c_str());
		}
	}
	auto operation_val = obj.GetMember("operation");
	if (!operation_val.IsValid()) {
		return "CommitReport required property 'operation' is missing";
	} else {
		if (json_utils::IsString(operation_val)) {
			operation = json_utils::GetString(operation_val);
		} else {
			return StringUtil::Format("CommitReport property 'operation' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(operation_val).c_str());
		}
	}
	auto metrics_val = obj.GetMember("metrics");
	if (!metrics_val.IsValid()) {
		return "CommitReport required property 'metrics' is missing";
	} else {
		error = metrics.TryFromJSON(metrics_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_val = obj.GetMember("metadata");
	if (metadata_val.IsValid()) {
		unordered_map<string, string> metadata_tmp;
		if (metadata_val.IsObject()) {
			metadata_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("CommitReport property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				metadata_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "CommitReport property 'metadata_tmp' is not of type 'object'";
		}
		metadata = std::move(metadata_tmp);
	}
	return "";
}

void CommitReport::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: table-name
	auto table_name_json = writer.CreateString(table_name);
	obj.Add("table-name", table_name_json);

	// Serialize: snapshot-id
	auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id);
	obj.Add("snapshot-id", snapshot_id_json);

	// Serialize: sequence-number
	auto sequence_number_json = writer.CreateSignedInteger(sequence_number);
	obj.Add("sequence-number", sequence_number_json);

	// Serialize: operation
	auto operation_json = writer.CreateString(operation);
	obj.Add("operation", operation_json);

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

JSONMutableValue CommitReport::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
