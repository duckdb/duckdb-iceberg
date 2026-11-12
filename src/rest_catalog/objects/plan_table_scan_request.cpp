
#include "rest_catalog/objects/plan_table_scan_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

PlanTableScanRequest::PlanTableScanRequest() {
}

PlanTableScanRequest PlanTableScanRequest::FromJSON(JSONValue obj) {
	PlanTableScanRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PlanTableScanRequest PlanTableScanRequest::Copy() const {
	PlanTableScanRequest res;
	if (snapshot_id.has_value()) {
		res.snapshot_id.emplace();
		(*res.snapshot_id) = (*snapshot_id);
	}
	if (select.has_value()) {
		res.select.emplace();
		(*res.select).reserve((*select).size());
		for (auto &item : (*select)) {
			(*res.select).emplace_back(item.Copy());
		}
	}
	if (filter != nullptr) {
		res.filter = filter ? make_uniq<Predicate>(filter->Copy()) : nullptr;
	}
	if (min_rows_requested.has_value()) {
		res.min_rows_requested.emplace();
		(*res.min_rows_requested) = (*min_rows_requested);
	}
	if (case_sensitive.has_value()) {
		res.case_sensitive.emplace();
		(*res.case_sensitive) = (*case_sensitive);
	}
	if (use_snapshot_schema.has_value()) {
		res.use_snapshot_schema.emplace();
		(*res.use_snapshot_schema) = (*use_snapshot_schema);
	}
	if (start_snapshot_id.has_value()) {
		res.start_snapshot_id.emplace();
		(*res.start_snapshot_id) = (*start_snapshot_id);
	}
	if (end_snapshot_id.has_value()) {
		res.end_snapshot_id.emplace();
		(*res.end_snapshot_id) = (*end_snapshot_id);
	}
	if (stats_fields.has_value()) {
		res.stats_fields.emplace();
		(*res.stats_fields).reserve((*stats_fields).size());
		for (auto &item : (*stats_fields)) {
			(*res.stats_fields).emplace_back(item.Copy());
		}
	}
	return res;
}

string PlanTableScanRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto snapshot_id_val = obj.GetMember("snapshot-id");
	if (snapshot_id_val.IsValid()) {
		int64_t snapshot_id_tmp;
		if (json_utils::IsInteger(snapshot_id_val)) {
			snapshot_id_tmp = json_utils::GetSignedInteger(snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(snapshot_id_val)) {
			snapshot_id_tmp = json_utils::GetUnsignedInteger(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'snapshot_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(snapshot_id_val).c_str());
		}
		snapshot_id = std::move(snapshot_id_tmp);
	}
	auto select_val = obj.GetMember("select");
	if (select_val.IsValid()) {
		vector<FieldName> select_tmp;
		if (select_val.IsArray()) {
			select_val.IterateArray([&](JSONValue select_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				FieldName select_tmp_item;
				error = select_tmp_item.TryFromJSON(select_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				select_tmp.emplace_back(std::move(select_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'select_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(select_val).c_str());
		}
		select = std::move(select_tmp);
	}
	auto filter_val = obj.GetMember("filter");
	if (filter_val.IsValid()) {
		filter = make_uniq<Predicate>();
		error = filter->TryFromJSON(filter_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto min_rows_requested_val = obj.GetMember("min-rows-requested");
	if (min_rows_requested_val.IsValid()) {
		int64_t min_rows_requested_tmp;
		if (json_utils::IsInteger(min_rows_requested_val)) {
			min_rows_requested_tmp = json_utils::GetSignedInteger(min_rows_requested_val);
		} else if (json_utils::IsUnsignedInteger(min_rows_requested_val)) {
			min_rows_requested_tmp = json_utils::GetUnsignedInteger(min_rows_requested_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'min_rows_requested_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(min_rows_requested_val).c_str());
		}
		min_rows_requested = std::move(min_rows_requested_tmp);
	}
	auto case_sensitive_val = obj.GetMember("case-sensitive");
	if (case_sensitive_val.IsValid()) {
		bool case_sensitive_tmp;
		if (json_utils::IsBoolean(case_sensitive_val)) {
			case_sensitive_tmp = json_utils::GetBoolean(case_sensitive_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'case_sensitive_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(case_sensitive_val).c_str());
		}
		case_sensitive = std::move(case_sensitive_tmp);
	}
	auto use_snapshot_schema_val = obj.GetMember("use-snapshot-schema");
	if (use_snapshot_schema_val.IsValid()) {
		bool use_snapshot_schema_tmp;
		if (json_utils::IsBoolean(use_snapshot_schema_val)) {
			use_snapshot_schema_tmp = json_utils::GetBoolean(use_snapshot_schema_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'use_snapshot_schema_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(use_snapshot_schema_val).c_str());
		}
		use_snapshot_schema = std::move(use_snapshot_schema_tmp);
	}
	auto start_snapshot_id_val = obj.GetMember("start-snapshot-id");
	if (start_snapshot_id_val.IsValid()) {
		int64_t start_snapshot_id_tmp;
		if (json_utils::IsInteger(start_snapshot_id_val)) {
			start_snapshot_id_tmp = json_utils::GetSignedInteger(start_snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(start_snapshot_id_val)) {
			start_snapshot_id_tmp = json_utils::GetUnsignedInteger(start_snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'start_snapshot_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(start_snapshot_id_val).c_str());
		}
		start_snapshot_id = std::move(start_snapshot_id_tmp);
	}
	auto end_snapshot_id_val = obj.GetMember("end-snapshot-id");
	if (end_snapshot_id_val.IsValid()) {
		int64_t end_snapshot_id_tmp;
		if (json_utils::IsInteger(end_snapshot_id_val)) {
			end_snapshot_id_tmp = json_utils::GetSignedInteger(end_snapshot_id_val);
		} else if (json_utils::IsUnsignedInteger(end_snapshot_id_val)) {
			end_snapshot_id_tmp = json_utils::GetUnsignedInteger(end_snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'end_snapshot_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(end_snapshot_id_val).c_str());
		}
		end_snapshot_id = std::move(end_snapshot_id_tmp);
	}
	auto stats_fields_val = obj.GetMember("stats-fields");
	if (stats_fields_val.IsValid()) {
		vector<FieldName> stats_fields_tmp;
		if (stats_fields_val.IsArray()) {
			stats_fields_val.IterateArray([&](JSONValue stats_fields_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				FieldName stats_fields_tmp_item;
				error = stats_fields_tmp_item.TryFromJSON(stats_fields_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				stats_fields_tmp.emplace_back(std::move(stats_fields_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'stats_fields_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(stats_fields_val).c_str());
		}
		stats_fields = std::move(stats_fields_tmp);
	}
	return "";
}

void PlanTableScanRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: snapshot-id
	if (snapshot_id.has_value()) {
		auto &snapshot_id_value = *snapshot_id;
		auto snapshot_id_json = writer.CreateSignedInteger(snapshot_id_value);
		obj.Add("snapshot-id", snapshot_id_json);
	}

	// Serialize: select
	if (select.has_value()) {
		auto &select_value = *select;
		auto select_json = writer.CreateArray();
		for (const auto &select_json_item : select_value) {
			auto select_json_item_json = select_json_item.ToJSON(writer);
			select_json.Append(select_json_item_json);
		}
		obj.Add("select", select_json);
	}

	// Serialize: filter
	if (filter != nullptr) {
		auto filter_json = filter->ToJSON(writer);
		obj.Add("filter", filter_json);
	}

	// Serialize: min-rows-requested
	if (min_rows_requested.has_value()) {
		auto &min_rows_requested_value = *min_rows_requested;
		auto min_rows_requested_json = writer.CreateSignedInteger(min_rows_requested_value);
		obj.Add("min-rows-requested", min_rows_requested_json);
	}

	// Serialize: case-sensitive
	if (case_sensitive.has_value()) {
		auto &case_sensitive_value = *case_sensitive;
		auto case_sensitive_json = writer.CreateBoolean(case_sensitive_value);
		obj.Add("case-sensitive", case_sensitive_json);
	}

	// Serialize: use-snapshot-schema
	if (use_snapshot_schema.has_value()) {
		auto &use_snapshot_schema_value = *use_snapshot_schema;
		auto use_snapshot_schema_json = writer.CreateBoolean(use_snapshot_schema_value);
		obj.Add("use-snapshot-schema", use_snapshot_schema_json);
	}

	// Serialize: start-snapshot-id
	if (start_snapshot_id.has_value()) {
		auto &start_snapshot_id_value = *start_snapshot_id;
		auto start_snapshot_id_json = writer.CreateSignedInteger(start_snapshot_id_value);
		obj.Add("start-snapshot-id", start_snapshot_id_json);
	}

	// Serialize: end-snapshot-id
	if (end_snapshot_id.has_value()) {
		auto &end_snapshot_id_value = *end_snapshot_id;
		auto end_snapshot_id_json = writer.CreateSignedInteger(end_snapshot_id_value);
		obj.Add("end-snapshot-id", end_snapshot_id_json);
	}

	// Serialize: stats-fields
	if (stats_fields.has_value()) {
		auto &stats_fields_value = *stats_fields;
		auto stats_fields_json = writer.CreateArray();
		for (const auto &stats_fields_json_item : stats_fields_value) {
			auto stats_fields_json_item_json = stats_fields_json_item.ToJSON(writer);
			stats_fields_json.Append(stats_fields_json_item_json);
		}
		obj.Add("stats-fields", stats_fields_json);
	}
}

JSONMutableValue PlanTableScanRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
