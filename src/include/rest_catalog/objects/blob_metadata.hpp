
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class BlobMetadata {
public:
	BlobMetadata();
	BlobMetadata(const BlobMetadata &) = delete;
	BlobMetadata &operator=(const BlobMetadata &) = delete;
	BlobMetadata(BlobMetadata &&) = default;
	BlobMetadata &operator=(BlobMetadata &&) = default;

public:
	// Deserialization
	static BlobMetadata FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	BlobMetadata Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int64_t snapshot_id;
	int64_t sequence_number;
	vector<int32_t> fields;
	optional<unordered_map<string, string>> properties;
};

} // namespace rest_api_objects
} // namespace duckdb
