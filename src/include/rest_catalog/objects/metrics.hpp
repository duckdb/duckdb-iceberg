
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/metric_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Metrics {
public:
	Metrics();
	Metrics(const Metrics &) = delete;
	Metrics &operator=(const Metrics &) = delete;
	Metrics(Metrics &&) = default;
	Metrics &operator=(Metrics &&) = default;

public:
	static Metrics FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	case_insensitive_map_t<MetricResult> additional_properties;
};

} // namespace rest_api_objects
} // namespace duckdb
