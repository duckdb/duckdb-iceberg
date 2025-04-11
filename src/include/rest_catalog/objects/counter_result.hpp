
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CounterResult {
public:
	CounterResult();
	CounterResult(const CounterResult &) = delete;
	CounterResult &operator=(const CounterResult &) = delete;
	CounterResult(CounterResult &&) = default;
	CounterResult &operator=(CounterResult &&) = default;

public:
	static CounterResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string unit;
	int64_t value;
};

} // namespace rest_api_objects
} // namespace duckdb
