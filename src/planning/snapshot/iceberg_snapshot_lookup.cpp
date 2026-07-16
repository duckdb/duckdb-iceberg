#include "iceberg_options.hpp"

namespace duckdb {

IcebergSnapshotLookup IcebergSnapshotLookup::FromAtClause(optional_ptr<BoundAtClause> at) {
	if (!at) {
		return FromLatest();
	}

	auto &unit = at->Unit();
	auto &value = at->GetValue();

	if (value.IsNull()) {
		throw InvalidInputException("NULL values can not be used as the 'unit' of a time travel clause");
	}
	if (StringUtil::CIEquals(unit, "version")) {
		return FromSnapshotId(value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>());
	} else if (StringUtil::CIEquals(unit, "timestamp")) {
		return FromTimestamp(value.DefaultCastAs(LogicalType::TIMESTAMP_MS).GetValue<timestamp_ms_t>());
	} else {
		throw InvalidInputException(
		    "Unit '%s' for time travel is not valid, supported options are 'version' and 'timestamp'", unit);
	}
}

} // namespace duckdb
