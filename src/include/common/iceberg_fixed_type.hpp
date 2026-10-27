#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;
class LogicalOperator;

struct IcebergFixedType {
	static LogicalType Get(int32_t length);
	static LogicalType Parse(const string &type);
	static bool IsFixed(const LogicalType &type);
	static int32_t GetLength(const LogicalType &type);
	static void Register(ExtensionLoader &loader);
	//! Throws if a bound CREATE TABLE / CREATE TYPE / ALTER TABLE plan would store ICEBERG_FIXED in a non-Iceberg
	//! catalog. Called from the Iceberg planner's post-bind hook.
	static void RejectOutsideIceberg(ClientContext &context, LogicalOperator &op);
};

} // namespace duckdb
