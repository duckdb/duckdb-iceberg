#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include <roaring/roaring.hh>

namespace duckdb {

struct IcebergDeletionVector : public DeleteFilter {
public:
	IcebergDeletionVector() {
	}

public:
	static unique_ptr<IcebergDeletionVector> FromBlob(data_ptr_t blob_start, idx_t blob_length);

public:
	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

public:
	unordered_map<int32_t, roaring::Roaring> bitmaps;

	//! State shared between Filter calls
	roaring::BulkContext bulk_context;
	optional_ptr<roaring::Roaring> current_bitmap;
	bool has_current_high = false;
	//! High bits of the current bitmap (the key in the map)
	int32_t current_high;
};

} // namespace duckdb
