
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UnregisterTableResult {
public:
	UnregisterTableResult();
	UnregisterTableResult(const UnregisterTableResult &) = delete;
	UnregisterTableResult &operator=(const UnregisterTableResult &) = delete;
	UnregisterTableResult(UnregisterTableResult &&) = default;
	UnregisterTableResult &operator=(UnregisterTableResult &&) = default;

public:
	// Deserialization
	static UnregisterTableResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	UnregisterTableResult Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string metadata_location;
	TableMetadata metadata;
};

} // namespace rest_api_objects
} // namespace duckdb
