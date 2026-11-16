
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

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
	// Deserialization
	static ViewHistoryEntry FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ViewHistoryEntry Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t version_id;
	int64_t timestamp_ms;
};

} // namespace rest_api_objects
} // namespace duckdb
