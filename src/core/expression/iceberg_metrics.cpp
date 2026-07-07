#include "core/expression/iceberg_metrics.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

IcebergMetricsConfig ParseMetricsMode(const string &raw) {
	auto value = StringUtil::Lower(raw);
	if (value == "none") {
		return {IcebergMetricsMode::NONE, 0};
	}
	if (value == "counts") {
		return {IcebergMetricsMode::COUNTS, 0};
	}
	if (value == "full") {
		return {IcebergMetricsMode::FULL, DConstants::INVALID_INDEX};
	}
	if (StringUtil::StartsWith(value, "truncate(") && StringUtil::EndsWith(value, ")")) {
		auto inner = value.substr(9, value.size() - 10);
		try {
			auto length = std::stoull(inner);
			if (length > 0) {
				return {IcebergMetricsMode::TRUNCATE, static_cast<idx_t>(length)};
			}
		} catch (...) {
			// fall through to the error below
		}
		throw InvalidConfigurationException("Invalid metrics mode '%s': truncate length must be a positive integer",
		                                    raw);
	}
	throw InvalidConfigurationException(
	    "Invalid write.metadata.metrics mode '%s': expected 'none', 'counts', 'truncate(<n>)', or 'full'", raw);
}

IcebergMetricsConfig GetDefaultMetricsConfig(const IcebergTableMetadata &table_metadata) {
	auto prop = table_metadata.GetTableProperty("write.metadata.metrics.default");
	if (prop.empty()) {
		// Iceberg's default when unset is truncate(16).
		return IcebergMetricsConfig();
	}
	return ParseMetricsMode(prop);
}

IcebergMetricsConfig GetColumnMetricsConfig(const IcebergTableMetadata &table_metadata,
                                            const IcebergMetricsConfig &default_config, const string &column_name) {
	auto prop = table_metadata.GetTableProperty("write.metadata.metrics.column." + column_name);
	if (prop.empty()) {
		return default_config;
	}
	return ParseMetricsMode(prop);
}

} // namespace duckdb
