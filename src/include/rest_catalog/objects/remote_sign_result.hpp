
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/multi_valued_map.hpp"

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
	static RemoteSignResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemoteSignResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string uri;
	MultiValuedMap headers;
};

} // namespace rest_api_objects
} // namespace duckdb
