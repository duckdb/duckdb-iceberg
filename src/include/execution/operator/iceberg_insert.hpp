//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "execution/operator/physical_iceberg_create_table.hpp"

namespace duckdb {

enum class IcebergInsertVirtualColumns { NONE, WRITE_ROW_ID, WRITE_SEQUENCE_NUMBER, WRITE_ROW_ID_AND_SEQUENCE_NUMBER };

struct IcebergCopyInput {
	explicit IcebergCopyInput(ClientContext &context, const IcebergTableMetadata &table_metadata,
	                          const IcebergTableSchema &schema);

public:
	const IcebergTableMetadata &table_metadata;
	const IcebergTableSchema &schema;
	string data_path;
	//! Set of (key, value) options
	case_insensitive_map_t<vector<Value>> options;
	//! Partition specification for the table (if partitioned)
	optional_ptr<const IcebergPartitionSpec> partition_spec;
	//! Table index for logical plan generation (used when generating partition expressions)
	optional_idx get_table_index;
	IcebergInsertVirtualColumns virtual_columns = IcebergInsertVirtualColumns::NONE;
};

struct IcebergCopyOptions {
public:
	IcebergCopyOptions(unique_ptr<CopyInfo> info, CopyFunction copy_function);

public:
	unique_ptr<CopyInfo> info;
	CopyFunction copy_function;
	unique_ptr<FunctionData> bind_data;

	string file_path;
	FilenamePattern filename_pattern;
	string file_extension;
	CopyOverwriteMode overwrite_mode;
	bool per_thread_output;
	//! default target file size: 512MB, per the Iceberg spec default for write.target-file-size-bytes
	idx_t file_size_bytes = 512ULL * 1024 * 1024;
	//! Row group sizing. DuckDB's parquet copy function ignores the row_group_size(_bytes) copy options;
	//! they are resolved by the COPY binder into these fields on the LogicalCopyToFile instead.
	//! Row groups are flushed when either threshold is hit. Iceberg only defines a byte default
	//! (write.parquet.row-group-size-bytes = 128MB), so we leave batch_size unset unless the table
	//! sets write.parquet.row-group-size - otherwise DuckDB's 122880 row default would always win.
	optional_idx batch_size;
	optional_idx batch_size_bytes = 128ULL * 1024 * 1024;
	CopyFunctionReturnType return_type;

	//! Partitioning
	bool partition_output = false;
	bool write_partition_columns = true;
	bool write_empty_file = true;
	vector<idx_t> partition_columns;
	vector<BoundOrderByNode> order_columns;
	vector<Identifier> names;
	vector<LogicalType> expected_types;
	bool partitioned_paths = true;

	//! Set of projection columns to execute prior to inserting (if any)
	vector<unique_ptr<Expression>> projection_list;
};

class IcebergInsertGlobalState : public GlobalSinkState {
public:
	explicit IcebergInsertGlobalState(ClientContext &context);

public:
	void AddFiles(DataChunk &chunk, const string &table_name, const IcebergTableMetadata &table_metadata);

public:
	ClientContext &context;
	mutex lock;
	vector<IcebergManifestEntry> written_files;
	atomic<idx_t> insert_count;
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
	//! When set, this insert is part of an UPDATE: points to the delete operator so Finalize
	//! can call AddUpdateSnapshot instead of AddSnapshot.
	optional_ptr<PhysicalOperator> update_delete_op;
	//! When set, this insert is a CTAS whose table is created lazily by an
	//! upstream PhysicalIcebergCreateTable. Sink/Finalize resolve the
	//! TableCatalogEntry through this shared state instead of `table`.
	shared_ptr<IcebergCTASCreateState> create_state;

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
	                                           const IcebergCopyInput &copy_input, optional_ptr<PhysicalOperator> plan);
	static IcebergCopyOptions GetCopyOptions(ClientContext &context, const IcebergCopyInput &copy_input);

	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    IcebergTableEntry &table);
	static vector<IcebergManifestEntry> GetInsertManifestEntries(IcebergInsertGlobalState &global_state);
	static void AddWrittenFiles(IcebergInsertGlobalState &global_state, DataChunk &chunk,
	                            optional_ptr<TableCatalogEntry> table);

	//! Resolve the catalog entry this insert is targeting. For INSERT INTO this
	//! is just `this->table`; for CTAS the table is created lazily by an
	//! upstream PhysicalIcebergCreateTable, so we read it from `create_state`.
	optional_ptr<TableCatalogEntry> GetEffectiveTable() const;

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
