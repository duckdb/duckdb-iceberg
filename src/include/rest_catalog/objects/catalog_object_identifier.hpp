
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CatalogObjectIdentifier {
public:
	CatalogObjectIdentifier();
	CatalogObjectIdentifier(const CatalogObjectIdentifier &) = delete;
	CatalogObjectIdentifier &operator=(const CatalogObjectIdentifier &) = delete;
	CatalogObjectIdentifier(CatalogObjectIdentifier &&) = default;
	CatalogObjectIdentifier &operator=(CatalogObjectIdentifier &&) = default;

public:
	// Deserialization
	static CatalogObjectIdentifier FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	CatalogObjectIdentifier Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	vector<string> value;
};

} // namespace rest_api_objects
} // namespace duckdb
