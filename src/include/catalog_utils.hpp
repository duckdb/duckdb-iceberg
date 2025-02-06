
#pragma once

#include "duckdb.hpp"
#include "catalog_api.hpp"

namespace duckdb {
class ICSchemaEntry;
class ICTransaction;

enum class ICTypeAnnotation { STANDARD, CAST_TO_VARCHAR, NUMERIC_AS_DOUBLE, CTID, JSONB, FIXED_LENGTH_CHAR };

struct ICType {
	idx_t oid = 0;
	ICTypeAnnotation info = ICTypeAnnotation::STANDARD;
	vector<ICType> children;
};

class ICUtils {
public:
	static LogicalType ToUCType(const LogicalType &input);
	static LogicalType TypeToLogicalType(const string &columnDefinition);
	static string TypeToString(const LogicalType &input);
	static string LogicalToIcebergType(const LogicalType &input);
};

} // namespace duckdb
