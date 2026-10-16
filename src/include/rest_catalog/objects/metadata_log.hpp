
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class MetadataLog {
public:
	MetadataLog();
	MetadataLog(const MetadataLog &) = delete;
	MetadataLog &operator=(const MetadataLog &) = delete;
	MetadataLog(MetadataLog &&) = default;
	MetadataLog &operator=(MetadataLog &&) = default;
	class Object8 {
	public:
		Object8();
		Object8(const Object8 &) = delete;
		Object8 &operator=(const Object8 &) = delete;
		Object8(Object8 &&) = default;
		Object8 &operator=(Object8 &&) = default;

	public:
		// Deserialization
		static Object8 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object8 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		string metadata_file;
		int64_t timestamp_ms;
	};

public:
	// Deserialization
	static MetadataLog FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	MetadataLog Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<Object8> value;
};

} // namespace rest_api_objects
} // namespace duckdb
