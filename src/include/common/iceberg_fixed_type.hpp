#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

class ExtensionLoader;

struct IcebergFixedType {
	static LogicalType Get(int32_t length);
	static LogicalType Parse(const string &type);
	static bool IsFixed(const LogicalType &type);
	static int32_t GetLength(const LogicalType &type);
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
