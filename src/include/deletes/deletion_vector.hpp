#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "deletes/iceberg_delete_data.hpp"
#include <roaring/roaring.hh>

namespace duckdb {

struct IcebergDeletionVectorData : public enable_shared_from_this<IcebergDeletionVectorData>, IcebergDeleteData {
public:
	IcebergDeletionVectorData(const IcebergManifestEntry &entry)
	    : IcebergDeleteData(IcebergDeleteType::DELETION_VECTOR, entry) {
	}
	virtual ~IcebergDeletionVectorData() override {
	}

public:
	static shared_ptr<IcebergDeletionVectorData> FromBlob(const IcebergManifestEntry &entry, data_ptr_t blob_start,
	                                                      idx_t blob_length);
	vector<data_t> ToBlob() const;

public:
	unique_ptr<DeleteFilter> ToFilter() const override;
	void ToSet(set<idx_t> &out) const override;

public:
	unordered_map<int32_t, roaring::Roaring> bitmaps;
};

struct IcebergDeletionVector : public DeleteFilter {
public:
	IcebergDeletionVector(shared_ptr<const IcebergDeletionVectorData> data) : data(data) {
	}

public:
	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

public:
	//! Immutable state of the deletion vector
	shared_ptr<const IcebergDeletionVectorData> data;
	//! State shared between Filter calls
	roaring::BulkContext bulk_context;
	optional_ptr<const roaring::Roaring> current_bitmap = nullptr;
	bool has_current_high = false;
	//! High bits of the current bitmap (the key in the map)
	int32_t current_high;
};

} // namespace duckdb
