#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class IcebergSnapshot;

struct IcebergSnapshotScanInfo {
	//! The snapshot to scan
	optional_ptr<const IcebergSnapshot> snapshot;
	//! The schema id assigned to the scan (either from table metadata or the snapshot)
	int32_t schema_id = -1;
	//! Whether this is the transaction-visible head of 'main', so uncommitted writes of the
	//! current transaction must be included (a plain read, or an explicit read of 'main')
	bool is_transaction_head = false;
};

} // namespace duckdb
