
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/counter_result.hpp"
#include "rest_catalog/objects/timer_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MetricResult {
public:
	MetricResult();
	MetricResult(const MetricResult &) = delete;
	MetricResult &operator=(const MetricResult &) = delete;
	MetricResult(MetricResult &&) = default;
	MetricResult &operator=(MetricResult &&) = default;

public:
	static MetricResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	CounterResult counter_result;
	bool has_counter_result = false;
	TimerResult timer_result;
	bool has_timer_result = false;
};

} // namespace rest_api_objects
} // namespace duckdb
