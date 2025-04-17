
#include "rest_catalog/objects/report_metrics_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ReportMetricsRequest::ReportMetricsRequest() {
}

ReportMetricsRequest ReportMetricsRequest::FromJSON(yyjson_val *obj) {
	ReportMetricsRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ReportMetricsRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	error = scan_report.TryFromJSON(obj);
	if (error.empty()) {
		has_scan_report = true;
	}
	error = commit_report.TryFromJSON(obj);
	if (error.empty()) {
		has_commit_report = true;
	}
	if (!has_commit_report && !has_scan_report) {
		return "ReportMetricsRequest failed to parse, none of the anyOf candidates matched";
	}
	auto report_type_val = yyjson_obj_get(obj, "report-type");
	if (!report_type_val) {
		return "ReportMetricsRequest required property 'report-type' is missing";
	} else {
		if (yyjson_is_str(report_type_val)) {
			report_type = yyjson_get_str(report_type_val);
		} else {
			return StringUtil::Format(
			    "ReportMetricsRequest property 'report_type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(report_type_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
