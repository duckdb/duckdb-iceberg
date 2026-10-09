
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/labels.hpp"
#include "rest_catalog/objects/view_metadata.hpp"

namespace duckdb {
namespace rest_api_objects {

class LoadViewResult {
public:
	LoadViewResult();
	LoadViewResult(const LoadViewResult &) = delete;
	LoadViewResult &operator=(const LoadViewResult &) = delete;
	LoadViewResult(LoadViewResult &&) = default;
	LoadViewResult &operator=(LoadViewResult &&) = default;

public:
	// Deserialization
	static LoadViewResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	LoadViewResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string metadata_location;
	ViewMetadata metadata;
	optional<case_insensitive_map_t<string>> config;
	optional<Labels> labels;
};

} // namespace rest_api_objects
} // namespace duckdb
