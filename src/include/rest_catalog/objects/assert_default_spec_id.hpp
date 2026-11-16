
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

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
	static AssertDefaultSpecId FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertDefaultSpecId Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int32_t default_spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
