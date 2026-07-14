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

//! Re-applies an access-delegation provider's mandatory scan filter (stored on IcebergScanInfo) above
//! the iceberg scan as a LogicalFilter. This is a generic, provider-agnostic safety net: it ensures the
//! filter is enforced even if bind-time table filters were elided or a user predicate tried to bypass it.
class DelegationRowFilterOptimizer {
public:
	ClientContext &context;

public:
	DelegationRowFilterOptimizer(ClientContext &context);
	void VisitOperator(unique_ptr<LogicalOperator> &op);
};

class IcebergOptimizer {
public:
	static OptimizerExtension Create();
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
