//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_delete_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/iceberg_metadata_info.hpp"

namespace duckdb {

struct IcebergDeleteData {
	vector<idx_t> deleted_rows;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) const;
};

class IcebergDeleteFilter : public DeleteFilter {
public:
	IcebergDeleteFilter();

	shared_ptr<IcebergDeleteData> delete_data;
	optional_idx max_row_count;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;
	void Initialize(ClientContext &context, const IcebergFileData &delete_file);
	void SetMaxRowCount(idx_t max_row_count);

private:
	static vector<idx_t> ScanDeleteFile(ClientContext &context, const IcebergFileData &delete_file);
};

} // namespace duckdb
