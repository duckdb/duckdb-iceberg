
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/content_file.hpp"
#include "rest_catalog/objects/count_map.hpp"
#include "rest_catalog/objects/value_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class DataFile {
public:
	DataFile();
	DataFile(const DataFile &) = delete;
	DataFile &operator=(const DataFile &) = delete;
	DataFile(DataFile &&) = default;
	DataFile &operator=(DataFile &&) = default;

public:
	// Deserialization
	static DataFile FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	DataFile Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ContentFile content_file;
	optional<int64_t> first_row_id;
	optional<CountMap> column_sizes;
	optional<CountMap> value_counts;
	optional<CountMap> null_value_counts;
	optional<CountMap> nan_value_counts;
	optional<ValueMap> lower_bounds;
	optional<ValueMap> upper_bounds;
};

} // namespace rest_api_objects
} // namespace duckdb
