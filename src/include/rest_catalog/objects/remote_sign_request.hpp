
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/multi_valued_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemoteSignRequest {
public:
	RemoteSignRequest();
	RemoteSignRequest(const RemoteSignRequest &) = delete;
	RemoteSignRequest &operator=(const RemoteSignRequest &) = delete;
	RemoteSignRequest(RemoteSignRequest &&) = default;
	RemoteSignRequest &operator=(RemoteSignRequest &&) = default;

public:
	// Deserialization
	static RemoteSignRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemoteSignRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string region;
	string uri;
	string method;
	MultiValuedMap headers;
	optional<unordered_map<string, string>> properties;
	optional<string> body;
	optional<string> provider;
};

} // namespace rest_api_objects
} // namespace duckdb
