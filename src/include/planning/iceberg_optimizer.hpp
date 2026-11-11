//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_optimizer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ClientContext;
class LogicalOperator;
class BoundTableFunction;

class IcebergOptimizer {
public:
	static void TryEnableLateMaterialization(ClientContext &context, LogicalOperator &op);
	static void EnableLateMaterialization(BoundTableFunction &function);
};

} // namespace duckdb
