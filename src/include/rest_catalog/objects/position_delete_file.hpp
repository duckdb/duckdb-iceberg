
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/content_file.hpp"

namespace duckdb {
namespace rest_api_objects {

class PositionDeleteFile {
public:
	PositionDeleteFile();
	PositionDeleteFile(const PositionDeleteFile &) = delete;
	PositionDeleteFile &operator=(const PositionDeleteFile &) = delete;
	PositionDeleteFile(PositionDeleteFile &&) = default;
	PositionDeleteFile &operator=(PositionDeleteFile &&) = default;

public:
	// Deserialization
	static PositionDeleteFile FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PositionDeleteFile Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ContentFile content_file;
	optional<int64_t> content_offset;
	optional<int64_t> content_size_in_bytes;
};

} // namespace rest_api_objects
} // namespace duckdb
