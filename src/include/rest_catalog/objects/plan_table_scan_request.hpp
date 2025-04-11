
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/field_name.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression;

class PlanTableScanRequest {
public:
	PlanTableScanRequest();
	PlanTableScanRequest(const PlanTableScanRequest &) = delete;
	PlanTableScanRequest &operator=(const PlanTableScanRequest &) = delete;
	PlanTableScanRequest(PlanTableScanRequest &&) = default;
	PlanTableScanRequest &operator=(PlanTableScanRequest &&) = default;

public:
	static PlanTableScanRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t snapshot_id;
	bool has_snapshot_id = false;
	vector<FieldName> select;
	bool has_select = false;
	unique_ptr<Expression> filter;
	bool has_filter = false;
	bool case_sensitive;
	bool has_case_sensitive = false;
	bool use_snapshot_schema;
	bool has_use_snapshot_schema = false;
	int64_t start_snapshot_id;
	bool has_start_snapshot_id = false;
	int64_t end_snapshot_id;
	bool has_end_snapshot_id = false;
	vector<FieldName> stats_fields;
	bool has_stats_fields = false;
};

} // namespace rest_api_objects
} // namespace duckdb
