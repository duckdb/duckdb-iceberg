
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/commit_report.hpp"
#include "rest_catalog/objects/scan_report.hpp"

namespace duckdb {
namespace rest_api_objects {

class ReportMetricsRequest {
public:
	ReportMetricsRequest();
	ReportMetricsRequest(const ReportMetricsRequest &) = delete;
	ReportMetricsRequest &operator=(const ReportMetricsRequest &) = delete;
	ReportMetricsRequest(ReportMetricsRequest &&) = default;
	ReportMetricsRequest &operator=(ReportMetricsRequest &&) = default;

public:
	// Deserialization
	static ReportMetricsRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ReportMetricsRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<ScanReport> scan_report;
	optional<CommitReport> commit_report;
	string report_type;
};

} // namespace rest_api_objects
} // namespace duckdb
