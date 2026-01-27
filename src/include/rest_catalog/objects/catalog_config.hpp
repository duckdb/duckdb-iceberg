
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CatalogConfig {
public:
	CatalogConfig();
	CatalogConfig(const CatalogConfig &) = delete;
	CatalogConfig &operator=(const CatalogConfig &) = delete;
	CatalogConfig(CatalogConfig &&) = default;
	CatalogConfig &operator=(CatalogConfig &&) = default;

public:
	static CatalogConfig FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	case_insensitive_map_t<string> defaults;
	case_insensitive_map_t<string> overrides;
	vector<string> endpoints;
	bool has_endpoints = false;
	string idempotency_key_lifetime;
	bool has_idempotency_key_lifetime = false;
};

} // namespace rest_api_objects
} // namespace duckdb
