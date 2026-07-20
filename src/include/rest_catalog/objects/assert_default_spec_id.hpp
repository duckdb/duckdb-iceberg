
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertDefaultSpecId {
public:
	AssertDefaultSpecId();
	AssertDefaultSpecId(const AssertDefaultSpecId &) = delete;
	AssertDefaultSpecId &operator=(const AssertDefaultSpecId &) = delete;
	AssertDefaultSpecId(AssertDefaultSpecId &&) = default;
	AssertDefaultSpecId &operator=(AssertDefaultSpecId &&) = default;

public:
	// Deserialization
	static AssertDefaultSpecId FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	AssertDefaultSpecId Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string type;
	int32_t default_spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
