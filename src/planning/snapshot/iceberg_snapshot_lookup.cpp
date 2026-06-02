#include "iceberg_options.hpp"

namespace duckdb {

IcebergSnapshotLookup IcebergSnapshotLookup::FromAtClause(optional_ptr<BoundAtClause> at) {
	IcebergSnapshotLookup result;
	if (!at) {
		return result;
	}

	auto &unit = at->Unit();
	auto &value = at->GetValue();

	if (value.IsNull()) {
		throw InvalidInputException("NULL values can not be used as the 'unit' of a time travel clause");
	}
	if (StringUtil::CIEquals(unit, "version")) {
		result.snapshot_source = SnapshotSource::FROM_ID;
		result.snapshot_id = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
	} else if (StringUtil::CIEquals(unit, "timestamp")) {
		result.snapshot_source = SnapshotSource::FROM_TIMESTAMP;
		result.snapshot_timestamp = value.DefaultCastAs(LogicalType::TIMESTAMP).GetValue<timestamp_t>();
	} else {
		throw InvalidInputException(
		    "Unit '%s' for time travel is not valid, supported options are 'version' and 'timestamp'", unit);
	}
	return result;
}

} // namespace duckdb
