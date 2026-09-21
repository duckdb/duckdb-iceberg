#include "core/expression/iceberg_predicate_stats.hpp"

#include "core/expression/iceberg_value.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

void IcebergPredicateStats::SetLowerBound(const Value &new_lower_bound) {
	lower_bound = new_lower_bound;
}

void IcebergPredicateStats::SetUpperBound(const Value &new_upper_bound) {
	upper_bound = new_upper_bound;
}

bool IcebergPredicateStats::BoundsAreNull() const {
	return lower_bound && upper_bound && lower_bound->IsNull() && upper_bound->IsNull();
}

static shared_ptr<BaseStatistics> BuildGeometryStats(const Value &lower_bound, const Value &upper_bound,
                                                     const LogicalType &type) {
	if (lower_bound.IsNull() || upper_bound.IsNull()) {
		return nullptr;
	}
	auto lower_blob = lower_bound.GetValueUnsafe<string_t>();
	auto upper_blob = upper_bound.GetValueUnsafe<string_t>();
	const auto lower_coordinate_card = lower_blob.GetSize() / sizeof(double);
	const auto upper_coordinate_card = upper_blob.GetSize() / sizeof(double);
	if (lower_coordinate_card < 2 || upper_coordinate_card < 2) {
		return nullptr;
	}
	const auto *lo = reinterpret_cast<const double *>(lower_blob.GetData());
	const auto *hi = reinterpret_cast<const double *>(upper_blob.GetData());

	auto stats = make_shared_ptr<BaseStatistics>(GeometryStats::CreateUnknown(type));
	auto &extent = GeometryStats::GetExtent(*stats);
	extent.x_min = lo[0];
	extent.y_min = lo[1];
	extent.x_max = hi[0];
	extent.y_max = hi[1];
	if (lower_coordinate_card >= 3 && upper_coordinate_card >= 3) {
		extent.z_min = lo[2];
		extent.z_max = hi[2];
	}
	if (lower_coordinate_card >= 4 && upper_coordinate_card >= 4) {
		extent.m_min = lo[3];
		extent.m_max = hi[3];
	}
	return stats;
}

IcebergPredicateStats IcebergPredicateStats::DeserializeBounds(const Value &lower_bound, const Value &upper_bound,
                                                               const string &name, const LogicalType &type) {
	IcebergPredicateStats result;
	if (type.id() == LogicalTypeId::GEOMETRY) {
		result.geometry_stats = BuildGeometryStats(lower_bound, upper_bound, type);
		if (!result.geometry_stats) {
			result.lower_bound.reset();
			result.upper_bound.reset();
		}
		return result;
	}

	if (!lower_bound.IsNull()) {
		D_ASSERT(lower_bound.type().id() == LogicalTypeId::BLOB);
		auto blob = lower_bound.GetValueUnsafe<string_t>();
		// Truncated UTF-8 string metrics are not a safe zonemap bound (especially
		// upper bounds). Omit them rather than failing the whole scan / stats table.
		if (type.id() == LogicalTypeId::VARCHAR && !Utf8Proc::IsValid(blob.GetData(), blob.GetSize())) {
			// skip
		} else {
			auto deserialized = IcebergValue::DeserializeValue(blob, type);
			if (deserialized.HasError()) {
				throw InvalidConfigurationException("Column %s lower bound deserialization failed: %s", name,
				                                    deserialized.GetError());
			}
			result.SetLowerBound(deserialized.GetValue());
		}
	}
	if (!upper_bound.IsNull()) {
		D_ASSERT(upper_bound.type().id() == LogicalTypeId::BLOB);
		auto blob = upper_bound.GetValueUnsafe<string_t>();
		if (type.id() == LogicalTypeId::VARCHAR && !Utf8Proc::IsValid(blob.GetData(), blob.GetSize())) {
			// skip
		} else {
			auto deserialized = IcebergValue::DeserializeValue(blob, type);
			if (deserialized.HasError()) {
				throw InvalidConfigurationException("Column %s upper bound deserialization failed: %s", name,
				                                    deserialized.GetError());
			}
			result.SetUpperBound(deserialized.GetValue());
		}
	}
	return result;
}

} // namespace duckdb
