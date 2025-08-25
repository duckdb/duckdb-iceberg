#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

namespace duckdb {

struct IcebergPositionalDeleteData : public DeleteFilter {
public:
	IcebergPositionalDeleteData() {
	}

public:
	void AddRow(int64_t row_id) {
		temp_invalid_rows.insert(row_id);
	}

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override {
		if (count == 0) {
			return 0;
		}
		result_sel.Initialize(STANDARD_VECTOR_SIZE);
		idx_t selection_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!temp_invalid_rows.count(i + start_row_index)) {
				result_sel.set_index(selection_idx++, i);
			}
		}
		return selection_idx;
	}

public:
	//! Store invalid rows here before finalizing into a SelectionVector
	unordered_set<int64_t> temp_invalid_rows;
};

} // namespace duckdb
