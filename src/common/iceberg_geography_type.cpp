#include "common/iceberg_geography_type.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {

LogicalType IcebergGeographyType::Get(const string &iceberg_type) {
	//! `geography`, or `geography(<crs>)`, or `geography(<crs>, <algorithm>)`
	if (iceberg_type != "geography" &&
	    !(StringUtil::StartsWith(iceberg_type, "geography(") && iceberg_type.back() == ')')) {
		throw InvalidConfigurationException("Invalid geography type format: %s", iceberg_type);
	}
	return LogicalType(LogicalTypeId::BLOB).WithAlias(iceberg_type);
}

bool IcebergGeographyType::IsGeography(const LogicalType &type) {
	return type.id() == LogicalTypeId::BLOB && type.HasAlias() && StringUtil::StartsWith(type.GetAlias(), "geography");
}

bool IcebergGeographyType::ContainsGeography(const LogicalType &type) {
	return TypeVisitor::Contains(type, IsGeography);
}

string IcebergGeographyType::GetIcebergType(const LogicalType &type) {
	D_ASSERT(IsGeography(type));
	return type.GetAlias();
}

} // namespace duckdb
