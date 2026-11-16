#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! JSON maps retain exact keys. These lookups are only for interpreting recognized settings.
struct IcebergProperties {
	//! Prefer the lowercase key, then an unambiguous case-insensitive spelling.
	static unordered_map<string, string>::const_iterator Find(const unordered_map<string, string> &properties,
	                                                          const string &name);
	//! Configuration layers reject equivalent names instead of choosing by iteration order.
	static unordered_map<string, string>::const_iterator FindConfig(const unordered_map<string, string> &properties,
	                                                                const string &name);
};

} // namespace duckdb
