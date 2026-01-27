//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_schema_entry.hpp"

namespace duckdb {

struct IcebergCopyInput {
	explicit IcebergCopyInput(ClientContext &context, ICTableEntry &table);
	IcebergCopyInput(ClientContext &context, IRCSchemaEntry &schema, const ColumnList &columns,
	                 const string &data_path_p);

	IRCatalog &catalog;
	const ColumnList &columns;
	string data_path;
	//! Set of (key, value) options
	case_insensitive_map_t<vector<Value>> options;
};

class IcebergInsertGlobalState : public GlobalSinkState {
public:
	explicit IcebergInsertGlobalState(ClientContext &context);
	ClientContext &context;
	mutex lock;
	vector<IcebergManifestEntry> written_files;
	atomic<idx_t> insert_count;
};

struct IcebergColumnStats {
	explicit IcebergColumnStats(LogicalType type_p) : type(std::move(type_p)) {
	}

	// Copy constructor
	IcebergColumnStats(const IcebergColumnStats &other);
	IcebergColumnStats &operator=(const IcebergColumnStats &other);
	IcebergColumnStats(IcebergColumnStats &&other) noexcept = default;
	IcebergColumnStats &operator=(IcebergColumnStats &&other) noexcept = default;

	LogicalType type;
	string min;
	string max;
	idx_t null_count = 0;
	idx_t column_size_bytes = 0;
	bool contains_nan = false;
	bool has_null_count = false;
	bool has_min = false;
	bool has_max = false;
	bool any_valid = true;
	bool has_contains_nan = false;
	bool has_column_size_bytes = false;

public:
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const IcebergColumnStats &new_stats);
	IcebergColumnStats Copy() const;

private:
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
};

class IcebergInsert : public PhysicalOperator {
public:
	//! INSERT INTO
	IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
	              physical_index_vector_t<idx_t> column_index_map);
	IcebergInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, TableCatalogEntry &table);

	//! CREATE TABLE AS
	IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
	              unique_ptr<BoundCreateTableInfo> info);

	//! The table to insert into
	optional_ptr<TableCatalogEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! column_index_map
	physical_index_vector_t<idx_t> column_index_map;
	//! The physical copy used internally by this insert
	unique_ptr<PhysicalOperator> physical_copy_to_file;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	static PhysicalOperator &PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                           IcebergCopyInput &copy_input, optional_ptr<PhysicalOperator> plan);

	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, ICTableEntry &table);
	static vector<IcebergManifestEntry> GetInsertManifestEntries(IcebergInsertGlobalState &global_state);
	static IcebergColumnStats ParseColumnStats(const LogicalType &type, const vector<Value> &col_stats,
	                                           ClientContext &context);
	static void AddWrittenFiles(IcebergInsertGlobalState &global_state, DataChunk &chunk,
	                            optional_ptr<TableCatalogEntry> table);

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
