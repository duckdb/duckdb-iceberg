#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "deletes/iceberg_delete_data.hpp"

namespace duckdb {

struct IcebergPositionalDeleteData : public enable_shared_from_this<IcebergPositionalDeleteData>, IcebergDeleteData {
public:
	IcebergPositionalDeleteData() {
	}
	virtual ~IcebergPositionalDeleteData() override {
	}

public:
	void AddRow(int64_t row_id) {
		invalid_rows.insert(row_id);
	}
	unique_ptr<DeleteFilter> ToFilter() const override;

public:
	//! Store invalid rows here before finalizing into a SelectionVector
	unordered_set<int64_t> invalid_rows;
};

struct IcebergPositionalDeleteFilter : public DeleteFilter {
public:
	IcebergPositionalDeleteFilter(shared_ptr<const IcebergPositionalDeleteData> data) : data(data) {
	}

public:
	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override {
		if (count == 0) {
			return 0;
		}
		result_sel.Initialize(STANDARD_VECTOR_SIZE);
		idx_t selection_idx = 0;
		auto &invalid_rows = data->invalid_rows;
		for (idx_t i = 0; i < count; i++) {
			if (!invalid_rows.count(i + start_row_index)) {
				result_sel.set_index(selection_idx++, i);
			}
		}
		return selection_idx;
	}

public:
	//! Immutable state of the positional delete
	shared_ptr<const IcebergPositionalDeleteData> data;
};

} // namespace duckdb
