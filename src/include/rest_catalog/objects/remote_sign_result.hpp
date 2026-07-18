
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/multi_valued_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemoteSignResult {
public:
	RemoteSignResult();
	RemoteSignResult(const RemoteSignResult &) = delete;
	RemoteSignResult &operator=(const RemoteSignResult &) = delete;
	RemoteSignResult(RemoteSignResult &&) = default;
	RemoteSignResult &operator=(RemoteSignResult &&) = default;

public:
	// Deserialization
	static RemoteSignResult FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RemoteSignResult Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string uri;
	MultiValuedMap headers;
};

} // namespace rest_api_objects
} // namespace duckdb
