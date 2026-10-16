//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_multi_file_list.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "planning/deletes/iceberg_delete_file_scanner.hpp"
#include "planning/scan_plan/iceberg_scan_planner.hpp"

namespace duckdb {

struct IcebergMultiFileList : public MultiFileList {
public:
	IcebergMultiFileList(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, const string &path,
	                     const IcebergOptions &options);
	~IcebergMultiFileList() override;

	unique_ptr<MultiFileList> DynamicFilterPushdown(MultiFileDynamicPushdownInfo &pushdown_info) const override;
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) const override;
	vector<OpenFileInfo> GetAllFiles() const override;
	FileExpandResult GetExpandResult() const override;
	idx_t GetTotalFileCount() const override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override;
	OpenFileInfo GetFile(idx_t i) const override;

	void SetTable(IcebergTableSchemaVersion &table);
	optional_ptr<IcebergTableSchemaVersion> GetTable() const;
	void Bind(vector<LogicalType> &return_types, vector<Identifier> &names);
	shared_ptr<IcebergDeleteData> GetExistingPositionalDeleteData(const string &file_path) const;
	IcebergDeletePlan ProcessDeletes(const IcebergFileScanTask &task) const;
	IcebergScanPlanner &GetScanPlanner();
	const IcebergScanPlanner &GetScanPlanner() const;

	IcebergDeleteExecutionState &GetDeleteReader() const;

private:
	IcebergMultiFileList(unique_ptr<IcebergScanPlanner> planner,
	                     shared_ptr<IcebergDeleteExecutionState> delete_execution);
	unique_ptr<IcebergMultiFileList> PushdownInternal(TableFilterSet &new_filters,
	                                                  const vector<ColumnIndex> &column_indexes) const;
	OpenFileInfo GetFileInternal(idx_t file_id) const;

private:
	unique_ptr<IcebergScanPlanner> planner;
	bool have_bound = false;
	vector<string> names;
	vector<LogicalType> types;
	//! Filtered MultiFileList views share execution caches just as they share metadata planning state.
	shared_ptr<IcebergDeleteExecutionState> delete_execution;
};

} // namespace duckdb
