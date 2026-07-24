#include "iceberg_options.hpp"
#include "common/iceberg_utils.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

IcebergOptions::IcebergOptions() : snapshot_lookup(IcebergSnapshotLookup::FromLatest()) {
}

IcebergOptions::IcebergOptions(named_parameter_map_t &named_parameters) : IcebergOptions() {
	snapshot_lookup.reset();

	for (auto &kv : named_parameters) {
		auto loption = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &val = kv.second;
		if (loption == "allow_moved_paths") {
			allow_moved_paths = BooleanValue::Get(val);
		} else if (loption == "metadata_compression_codec") {
			metadata_compression_codec = StringValue::Get(val);
		} else if (loption == "version") {
			table_version = StringValue::Get(val);
		} else if (loption == "version_name_format") {
			auto value = StringValue::Get(kv.second);
			auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
			if (string_substitutions != 2) {
				throw InvalidInputException(
				    "'version_name_format' has to contain two occurrences of '%s' in it, found %d", value,
				    string_substitutions);
			}
			version_name_format = value;
		} else if (loption == "snapshot_from_id") {
			if (snapshot_lookup) {
				throw InvalidInputException(
				    "Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
			}
			snapshot_lookup.emplace(IcebergSnapshotLookup::FromSnapshotId(val.GetValue<uint64_t>()));
		} else if (loption == "snapshot_from_timestamp") {
			if (snapshot_lookup) {
				throw InvalidInputException(
				    "Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
			}
			snapshot_lookup.emplace(IcebergSnapshotLookup::FromTimestamp(
			    val.DefaultCastAs(LogicalType::TIMESTAMP_MS).GetValue<timestamp_ms_t>()));
		}
	}
	if (!snapshot_lookup) {
		snapshot_lookup.emplace(IcebergSnapshotLookup::FromLatest());
	}
}

IcebergOptions &IcebergOptions::operator=(const IcebergOptions &other) {
	if (this == &other) {
		return *this;
	}
	allow_moved_paths = other.allow_moved_paths;
	metadata_compression_codec = other.metadata_compression_codec;
	infer_schema = other.infer_schema;
	table_version = other.table_version;
	version_name_format = other.version_name_format;
	snapshot_lookup.reset();
	if (other.snapshot_lookup) {
		snapshot_lookup.emplace(*other.snapshot_lookup);
	}
	return *this;
}

} // namespace duckdb
