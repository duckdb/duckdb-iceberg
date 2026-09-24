//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/planner_extension.hpp"

namespace duckdb {

class IcebergPlannerRoutine {
public:
	void VisitOperator(LogicalOperator &op, bool below_write = false);

private:
	void VisitScan(LogicalOperator &op, bool below_write);
};

class IcebergPlanner {
public:
	static PlannerExtension Create();
	static void PostBind(PlannerExtensionInput &input, BoundStatement &statement);
};

} // namespace duckdb
