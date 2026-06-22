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

class GuaranteeEqualityDeleteColumnsOptimizer {
public:
	ClientContext &context;

public:
	GuaranteeEqualityDeleteColumnsOptimizer(ClientContext &context);
	void VisitOperator(unique_ptr<LogicalOperator> &op);
};

class LakeFormationRowFilterOptimizer {
public:
	ClientContext &context;

public:
	LakeFormationRowFilterOptimizer(ClientContext &context);
	void VisitOperator(unique_ptr<LogicalOperator> &op);
};

class IcebergOptimizer {
public:
	static OptimizerExtension Create();
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
