#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "maintenance/table_lock_registry.hpp"

namespace duckdb {

//! Check that a name component contains only [a-zA-Z_][a-zA-Z0-9_]* — the
//! subset of identifiers that can appear unquoted in DuckDB SQL. Rejects
//! hyphens, unicode, spaces, and empty strings.
inline bool IsSimpleIdentifier(const string &part) {
	if (part.empty()) {
		return false;
	}
	for (idx_t i = 0; i < part.size(); ++i) {
		auto c = part[i];
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
			continue;
		}
		if (i > 0 && c >= '0' && c <= '9') {
			continue;
		}
		return false;
	}
	return true;
}

//! Split "catalog.schema.table" into a `MaintenanceTableKey`. Validates that
//! all three components are simple identifiers (no quoting, no nested
//! namespaces). Raises InvalidInputException on malformed input.
//!
//! Limitation: quoted identifiers and nested namespaces are not supported yet.
inline MaintenanceTableKey ParseMaintenanceTableIdentifier(const string &function_name, const string &identifier) {
	auto parts = StringUtil::Split(identifier, '.');
	if (parts.size() != 3) {
		throw InvalidInputException(
		    "%s: table identifier must be 'catalog.schema.table', got '%s'", function_name, identifier);
	}
	for (auto &part : parts) {
		if (part.empty()) {
			throw InvalidInputException(
			    "%s: table identifier '%s' has an empty component", function_name, identifier);
		}
		if (!IsSimpleIdentifier(part)) {
			throw InvalidInputException(
			    "%s currently requires a simple catalog.schema.table identifier; "
			    "quoted identifiers and nested namespaces are not supported yet (got '%s')",
			    function_name, identifier);
		}
	}
	//! Normalize to lower-case for consistent lock keys — DuckDB resolves
	//! unquoted identifiers case-insensitively.
	return MaintenanceTableKey{StringUtil::Lower(parts[0]), StringUtil::Lower(parts[1]),
	                           StringUtil::Lower(parts[2])};
}

} // namespace duckdb
