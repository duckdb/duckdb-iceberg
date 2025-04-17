
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TimerResult {
public:
	TimerResult();
	TimerResult(const TimerResult &) = delete;
	TimerResult &operator=(const TimerResult &) = delete;
	TimerResult(TimerResult &&) = default;
	TimerResult &operator=(TimerResult &&) = default;

public:
	static TimerResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string time_unit;
	int64_t count;
	int64_t total_duration;
};

} // namespace rest_api_objects
} // namespace duckdb
