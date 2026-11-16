
#include "rest_catalog/objects/report_metrics_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ReportMetricsRequest::ReportMetricsRequest() {
}

ReportMetricsRequest ReportMetricsRequest::FromJSON(JSONValue obj) {
	ReportMetricsRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ReportMetricsRequest ReportMetricsRequest::Copy() const {
	ReportMetricsRequest res;
	if (scan_report.has_value()) {
		res.scan_report.emplace();
		(*res.scan_report) = (*scan_report).Copy();
	}
	if (commit_report.has_value()) {
		res.commit_report.emplace();
		(*res.commit_report) = (*commit_report).Copy();
	}
	res.report_type = report_type;
	return res;
}

string ReportMetricsRequest::TryFromJSON(JSONValue obj) {
	string error;
	scan_report.emplace();
	error = scan_report->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		scan_report = nullopt;
	}
	commit_report.emplace();
	error = commit_report->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		commit_report = nullopt;
	}
	if (!(commit_report.has_value()) && !(scan_report.has_value())) {
		return "ReportMetricsRequest failed to parse, none of the anyOf candidates matched";
	}
	auto report_type_val = obj.GetMember("report-type");
	if (!report_type_val.IsValid()) {
		return "ReportMetricsRequest required property 'report-type' is missing";
	} else {
		if (json_utils::IsString(report_type_val)) {
			report_type = json_utils::GetString(report_type_val);
		} else {
			return StringUtil::Format(
			    "ReportMetricsRequest property 'report_type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(report_type_val).c_str());
		}
	}
	return "";
}

void ReportMetricsRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (scan_report.has_value()) {
		scan_report->PopulateJSON(writer, obj);
	} else if (commit_report.has_value()) {
		commit_report->PopulateJSON(writer, obj);
	}

	// Serialize: report-type
	auto report_type_json = writer.CreateString(report_type);
	obj.Add("report-type", report_type_json);
}

JSONMutableValue ReportMetricsRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
