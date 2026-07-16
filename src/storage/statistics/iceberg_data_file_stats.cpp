#include "storage/statistics/iceberg_data_file_stats.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/geometry.hpp"

#include "core/expression/iceberg_metrics.hpp"
#include "core/expression/iceberg_value.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "storage/statistics/iceberg_statistics.hpp"
#include "storage/statistics/iceberg_variant_statistics.hpp"

namespace duckdb {

namespace {

static bool IsMapType(const string &col_name, IcebergTableSchema &table_schema) {
	for (auto &col : table_schema.columns) {
		if (col->name == col_name) {
			if (col->type.id() == LogicalTypeId::MAP) {
				return true;
			}
		}
	}
	return false;
}

static string GetColumnNameBySourceId(const IcebergTableSchema &schema, idx_t source_id) {
	return schema.GetColumnByFieldId(source_id).name;
}

static string ParseQuotedValue(const string &input, idx_t &pos) {
	if (pos >= input.size() || input[pos] != '"') {
		throw InvalidInputException("Failed to parse quoted value - expected a quote");
	}
	string result;
	pos++;
	for (; pos < input.size(); pos++) {
		if (input[pos] == '"') {
			pos++;
			if (pos < input.size() && input[pos] == '"') {
				result += '"';
				continue;
			}
			return result;
		}
		result += input[pos];
	}
	throw InvalidInputException("Failed to parse quoted value - unterminated quote");
}

static vector<string> ParseQuotedList(const string &input, char list_separator) {
	vector<string> result;
	if (input.empty()) {
		return result;
	}
	idx_t pos = 0;
	while (true) {
		result.push_back(ParseQuotedValue(input, pos));
		if (pos >= input.size()) {
			break;
		}
		if (input[pos] != list_separator) {
			throw InvalidInputException("Failed to parse list - expected a %s", string(1, list_separator));
		}
		pos++;
	}
	return result;
}

} // namespace

void IcebergDataFileStats::PopulateFromReturnStats(ClientContext &context, IcebergDataFile &data_file,
                                                   const Value &column_stats,
                                                   const IcebergTableMetadata &table_metadata,
                                                   const string &table_name) {
	if (column_stats.IsNull()) {
		return;
	}

	auto table_current_schema_id = table_metadata.GetCurrentSchemaId();
	auto &ic_schema = table_metadata.GetSchemas().at(table_current_schema_id);
	auto &map_children = MapValue::GetChildren(column_stats);

	//! Variant columns emit one stats entry per shredded leaf — accumulate them
	//! per variant column and serialize bounds once all entries are seen.
	unordered_map<int32_t, IcebergVariantBounds> variant_bounds;
	auto default_metrics = GetDefaultMetricsConfig(table_metadata);

	for (idx_t col_idx = 0; col_idx < map_children.size(); col_idx++) {
		auto &struct_children = StructValue::GetChildren(map_children[col_idx]);
		auto &col_name = StringValue::Get(struct_children[0]);
		auto &col_stats = MapValue::GetChildren(struct_children[1]);
		auto column_names = ParseQuotedList(col_name, '.');
		if (column_names[0] == "_row_id") {
			continue;
		}

		optional_idx name_offset;
		auto column_info_p = ic_schema->GetFromPath(StringsToIdentifiers(column_names), &name_offset);
		if (!column_info_p) {
			auto normalized_col_name = StringUtil::Join(column_names, ".");
			throw InternalException("Column '%s' can not be found in the schema, but returned by RETURN_STATS",
			                        normalized_col_name);
		}
		if (name_offset.IsValid()) {
			variant_bounds[column_info_p->id].AddStatsEntry(column_names, name_offset.GetIndex(), col_stats);
			continue;
		}
		auto &column_info = *column_info_p;
		auto stats = IcebergColumnStats::ParseColumnStats(column_info.type, col_stats, context);

		//! Map types cannot violate NOT NULL; empty maps look like null maps.
		bool is_map = IsMapType(column_names[0], *ic_schema);
		if (!is_map && column_info.required && stats.null_count && *stats.null_count > 0) {
			auto normalized_col_name = StringUtil::Join(column_names, ".");
			throw ConstraintException("NOT NULL constraint failed: %s.%s", table_name, normalized_col_name);
		}

		auto metrics = GetColumnMetricsConfig(table_metadata, default_metrics, StringUtil::Join(column_names, "."));
		if (metrics.mode == IcebergMetricsMode::NONE) {
			continue;
		}
		const bool write_bounds =
		    metrics.mode == IcebergMetricsMode::TRUNCATE || metrics.mode == IcebergMetricsMode::FULL;

		if (write_bounds && stats.min) {
			auto serialized_value =
			    IcebergValue::SerializeValue(*stats.min, column_info.type, SerializeBound::LOWER_BOUND, metrics);
			if (serialized_value.HasError()) {
				throw InvalidConfigurationException(serialized_value.GetError());
			} else if (serialized_value.HasValue()) {
				data_file.lower_bounds[column_info.id] = serialized_value.GetValue();
			}
		}
		if (write_bounds && stats.max) {
			auto serialized_value =
			    IcebergValue::SerializeValue(*stats.max, column_info.type, SerializeBound::UPPER_BOUND, metrics);
			if (serialized_value.HasError()) {
				throw InvalidConfigurationException(serialized_value.GetError());
			} else if (serialized_value.HasValue()) {
				data_file.upper_bounds[column_info.id] = serialized_value.GetValue();
			}
		}
		//! Iceberg v3 Appendix D geometry bounding-box encoding.
		if (write_bounds && column_info.type.id() == LogicalTypeId::GEOMETRY && stats.has_bbox_xy) {
			vector<double> lower {stats.bbox_xmin, stats.bbox_ymin};
			vector<double> upper {stats.bbox_xmax, stats.bbox_ymax};
			if (stats.has_bbox_z) {
				lower.push_back(stats.bbox_zmin);
				upper.push_back(stats.bbox_zmax);
			} else if (stats.has_bbox_m) {
				//! Spark treats a 3-double bound as XYZ always; pad Z with
				//! ±infinity so XYM encodes unambiguously as 4D.
				const auto z_max = GeometryExtent::UNKNOWN_MAX;
				const auto z_min = GeometryExtent::UNKNOWN_MIN;
				lower.push_back(z_min);
				upper.push_back(z_max);
			}
			if (stats.has_bbox_m) {
				lower.push_back(stats.bbox_mmin);
				upper.push_back(stats.bbox_mmax);
			}
			const auto byte_count = lower.size() * sizeof(double);
			data_file.lower_bounds[column_info.id] = Value::BLOB(const_data_ptr_cast<double>(lower.data()), byte_count);
			data_file.upper_bounds[column_info.id] = Value::BLOB(const_data_ptr_cast<double>(upper.data()), byte_count);
		}
		if (stats.column_size_bytes) {
			data_file.column_sizes[column_info.id] = *stats.column_size_bytes;
		}
		if (stats.null_count) {
			data_file.null_value_counts[column_info.id] = *stats.null_count;
		}
		if (stats.num_values) {
			//! Iceberg value_counts includes nulls; Parquet num_values matches.
			data_file.value_counts[column_info.id] = *stats.num_values;
		}
	}

	for (auto &entry : variant_bounds) {
		auto variant_metrics =
		    GetColumnMetricsConfig(table_metadata, default_metrics, GetColumnNameBySourceId(*ic_schema, entry.first));
		if (variant_metrics.mode != IcebergMetricsMode::TRUNCATE && variant_metrics.mode != IcebergMetricsMode::FULL) {
			continue;
		}
		optional<string> lower_blob;
		optional<string> upper_blob;
		if (!entry.second.Finalize(context, lower_blob, upper_blob)) {
			continue;
		}
		if (lower_blob) {
			data_file.lower_bounds[entry.first] = Value::BLOB_RAW(*lower_blob);
		}
		if (upper_blob) {
			data_file.upper_bounds[entry.first] = Value::BLOB_RAW(*upper_blob);
		}
	}
}

} // namespace duckdb
