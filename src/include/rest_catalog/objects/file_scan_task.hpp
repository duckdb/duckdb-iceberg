
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/data_file.hpp"

namespace duckdb {
namespace rest_api_objects {

class Predicate;

class FileScanTask {
public:
	FileScanTask();
	FileScanTask(const FileScanTask &) = delete;
	FileScanTask &operator=(const FileScanTask &) = delete;
	FileScanTask(FileScanTask &&) = default;
	FileScanTask &operator=(FileScanTask &&) = default;

public:
	// Deserialization
	static FileScanTask FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FileScanTask Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	DataFile data_file;
	optional<vector<int32_t>> delete_file_references;
	unique_ptr<Predicate> residual_filter;
};

} // namespace rest_api_objects
} // namespace duckdb
