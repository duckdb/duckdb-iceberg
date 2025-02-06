
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/schema/physical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "storage/ic_catalog.hpp"
#include "storage/ic_schema_entry.hpp"

namespace duckdb {

class ICCreateTableAsOp : public PhysicalOperator {
public:
    ICCreateTableAsOp(vector<LogicalType> types,
                      unique_ptr<BoundCreateTableInfo> info, 
                      ICSchemaEntry *schemaEntry,
                      idx_t estimated_cardinality,
                      string &internal_name,
                      ICCredentials &credentials)
    : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE_AS, types, estimated_cardinality), 
          info(std::move(info)), 
          schemaEntry(schemaEntry),
          catalog_internal_name(internal_name),
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
    ICSchemaEntry *schemaEntry;
    ICCredentials &table_credentials;
};

}