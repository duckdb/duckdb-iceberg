
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

class RemoteSignRequest {
public:
	RemoteSignRequest();
	RemoteSignRequest(const RemoteSignRequest &) = delete;
	RemoteSignRequest &operator=(const RemoteSignRequest &) = delete;
	RemoteSignRequest(RemoteSignRequest &&) = default;
	RemoteSignRequest &operator=(RemoteSignRequest &&) = default;

public:
	// Deserialization
	static RemoteSignRequest FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	RemoteSignRequest Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string region;
	string uri;
	string method;
	MultiValuedMap headers;
	optional<case_insensitive_map_t<string>> properties;
	optional<string> body;
	optional<string> provider;
};

} // namespace rest_api_objects
} // namespace duckdb
