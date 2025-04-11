
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/metrics.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression;

class ScanReport {
public:
	ScanReport();
	ScanReport(const ScanReport &) = delete;
	ScanReport &operator=(const ScanReport &) = delete;
	ScanReport(ScanReport &&) = default;
	ScanReport &operator=(ScanReport &&) = default;

public:
	static ScanReport FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string table_name;
	int64_t snapshot_id;
	unique_ptr<Expression> filter;
	int64_t schema_id;
	vector<int64_t> projected_field_ids;
	vector<string> projected_field_names;
	Metrics metrics;
	case_insensitive_map_t<string> metadata;
	bool has_metadata = false;
};

} // namespace rest_api_objects
} // namespace duckdb
