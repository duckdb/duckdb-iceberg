#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

struct IcebergManifest;
class IcebergPendingManifest;
class IcebergSnapshot;

//! Owns row-ID allocation for one commit attempt, including rows inherited after an upgrade.
class IcebergRowIdAllocator {
public:
	explicit IcebergRowIdAllocator(int64_t table_next_row_id);
	IcebergRowIdAllocator(const IcebergRowIdAllocator &) = delete;
	IcebergRowIdAllocator &operator=(const IcebergRowIdAllocator &) = delete;

	//! Preserve an assigned range, or allocate one for an older data manifest.
	void AssignExistingManifest(IcebergManifest &manifest);
	//! Allocate the upper bound for newly written data. Delete manifests allocate nothing.
	optional<int64_t> AllocateManifest(const IcebergPendingManifest &manifest);
	//! Attach the complete allocation range and start the next snapshot at its end.
	void CompleteSnapshot(IcebergSnapshot &snapshot);
	int64_t NextRowId() const {
		return next_row_id;
	}

private:
	int64_t snapshot_start;
	int64_t next_row_id;
};

} // namespace duckdb
