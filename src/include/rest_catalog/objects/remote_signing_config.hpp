
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/multi_valued_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemoteSigningConfig {
public:
	RemoteSigningConfig();
	RemoteSigningConfig(const RemoteSigningConfig &) = delete;
	RemoteSigningConfig &operator=(const RemoteSigningConfig &) = delete;
	RemoteSigningConfig(RemoteSigningConfig &&) = default;
	RemoteSigningConfig &operator=(RemoteSigningConfig &&) = default;

public:
	// Deserialization
	static RemoteSigningConfig FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemoteSigningConfig Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<case_insensitive_map_t<string>> properties;
	optional<MultiValuedMap> headers;
};

} // namespace rest_api_objects
} // namespace duckdb
