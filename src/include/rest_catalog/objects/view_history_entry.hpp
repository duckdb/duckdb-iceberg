
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewHistoryEntry {
public:
	ViewHistoryEntry();
	ViewHistoryEntry(const ViewHistoryEntry &) = delete;
	ViewHistoryEntry &operator=(const ViewHistoryEntry &) = delete;
	ViewHistoryEntry(ViewHistoryEntry &&) = default;
	ViewHistoryEntry &operator=(ViewHistoryEntry &&) = default;

public:
	static ViewHistoryEntry FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t version_id;
	int64_t timestamp_ms;
};

} // namespace rest_api_objects
} // namespace duckdb
