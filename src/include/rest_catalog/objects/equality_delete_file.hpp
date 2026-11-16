
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/content_file.hpp"

namespace duckdb {
namespace rest_api_objects {

class EqualityDeleteFile {
public:
	EqualityDeleteFile();
	EqualityDeleteFile(const EqualityDeleteFile &) = delete;
	EqualityDeleteFile &operator=(const EqualityDeleteFile &) = delete;
	EqualityDeleteFile(EqualityDeleteFile &&) = default;
	EqualityDeleteFile &operator=(EqualityDeleteFile &&) = default;

public:
	// Deserialization
	static EqualityDeleteFile FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	EqualityDeleteFile Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ContentFile content_file;
	optional<vector<int32_t>> equality_ids;
};

} // namespace rest_api_objects
} // namespace duckdb
