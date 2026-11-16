
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class PageToken {
public:
	PageToken();
	PageToken(const PageToken &) = delete;
	PageToken &operator=(const PageToken &) = delete;
	PageToken(PageToken &&) = default;
	PageToken &operator=(PageToken &&) = default;

public:
	// Deserialization
	static PageToken FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PageToken Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
