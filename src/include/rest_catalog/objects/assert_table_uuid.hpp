
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertTableUUID {
public:
	AssertTableUUID();
	AssertTableUUID(const AssertTableUUID &) = delete;
	AssertTableUUID &operator=(const AssertTableUUID &) = delete;
	AssertTableUUID(AssertTableUUID &&) = default;
	AssertTableUUID &operator=(AssertTableUUID &&) = default;

public:
	// Deserialization
	static AssertTableUUID FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	AssertTableUUID Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string type;
	string uuid;
};

} // namespace rest_api_objects
} // namespace duckdb
