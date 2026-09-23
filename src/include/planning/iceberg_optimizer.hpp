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

class IcebergOptimizer {
public:
	static void TryEnableLateMaterialization(ClientContext &context, LogicalOperator &op);
};

} // namespace duckdb
