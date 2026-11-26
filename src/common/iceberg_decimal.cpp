#include "common/iceberg_decimal.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

constexpr const idx_t IcebergDecimal::MAX_PRECISION;

bool IcebergDecimal::IsSupported(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	return DecimalType::GetWidth(type) <= MAX_PRECISION;
}

void IcebergDecimal::Verify(const LogicalType &type) {
	if (IsSupported(type)) {
		return;
	}
	throw InvalidInputException("Iceberg decimals support at most %d digits of precision, got %s", MAX_PRECISION,
	                            type.ToString());
}

} // namespace duckdb
