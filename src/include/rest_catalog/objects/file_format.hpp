
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FileFormat {
public:
	FileFormat();
	FileFormat(const FileFormat &) = delete;
	FileFormat &operator=(const FileFormat &) = delete;
	FileFormat(FileFormat &&) = default;
	FileFormat &operator=(FileFormat &&) = default;

public:
	// Deserialization
	static FileFormat FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FileFormat Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
