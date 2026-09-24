//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class IcebergOptimizer {
public:
	static OptimizerExtension Create();
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
