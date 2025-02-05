
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/schema/physical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

class ICCreateTableAsOp : public PhysicalOperator {
public:
    ICCreateTableAsOp(vector<LogicalType> types,
                      unique_ptr<BoundCreateTableInfo> info, 
                      idx_t estimated_cardinality,
                      string &internal_name,
                      ICCatalogSet &catalog_set,
                      ICCredentials &credentials)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, types, estimated_cardinality), 
          info(std::move(info)), 
          catalog_internal_name(internal_name),
          catalog_set(catalog_set),
          table_credentials(credentials) {}

    // Override the Execute method
    OperatorResultType Execute(ExecutionContext &context, 
                               DataChunk &input, 
                               DataChunk &chunk,
                               GlobalOperatorState &gstate, 
                               OperatorState &state) const override;

private:
    unique_ptr<BoundCreateTableInfo> info;
    string &catalog_internal_name;
    ICCatalogSet &catalog_set;
    ICCredentials &table_credentials;
};

}