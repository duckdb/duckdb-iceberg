#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "storage/statistics/iceberg_variant_statistics.hpp"

namespace duckdb {

struct IcebergColumnStatsBindData : public TableFunctionData {
	unique_ptr<IcebergManifestList> iceberg_table;
	IcebergSnapshotScanInfo snapshot_to_scan;
	IcebergTableMetadata metadata;
	shared_ptr<IcebergTableSchema> schema;
	unordered_map<uint64_t, ColumnIndex> source_to_column_id;
};

struct IcebergColumnStatsGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	IcebergColumnStatsGlobalTableFunctionState(const IcebergColumnStatsBindData &bind_data) {
		column_it = bind_data.source_to_column_id.begin();
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergColumnStatsGlobalTableFunctionState>(
		    input.bind_data->Cast<IcebergColumnStatsBindData>());
	}

	idx_t current_manifest_idx = 0;
	idx_t current_manifest_entry_idx = 0;
	unordered_map<uint64_t, ColumnIndex>::const_iterator column_it;
};

//! GEOMETRY columns don't have a single scalar lower/upper bound; instead Iceberg stores the
//! min/max corner of the file's bounding box per coordinate axis (x, y, optional z, optional m).
//! There is no scalar string that represents this, so we serialize the corner as a JSON object
//! into the lower_bound / upper_bound columns. Callers that want a specific axis can cast the
//! string to JSON (or VARIANT) and select the key, e.g. (lower_bound::JSON ->> '$.bbox_x').
//! Absent Z/M axes are emitted as JSON null rather than ±infinity.
static string GeometryBoundJson(const GeometryExtent &extent, bool lower_corner) {
	auto number = [](double v) {
		return Value::DOUBLE(v).ToString();
	};
	auto bbox_x = number(lower_corner ? extent.x_min : extent.x_max);
	auto bbox_y = number(lower_corner ? extent.y_min : extent.y_max);
	auto bbox_z = extent.HasZ() ? number(lower_corner ? extent.z_min : extent.z_max) : "null";
	auto bbox_m = extent.HasM() ? number(lower_corner ? extent.m_min : extent.m_max) : "null";
	return StringUtil::Format("{\"bbox_x\":%s,\"bbox_y\":%s,\"bbox_z\":%s,\"bbox_m\":%s}", bbox_x, bbox_y, bbox_z,
	                          bbox_m);
}

static unique_ptr<FunctionData> IcebergColumnStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<IcebergColumnStatsBindData>();

	auto input_string = input.inputs[0].ToString();
	IcebergOptions options(input.named_parameters);
	auto resolved_metadata = IcebergUtils::ResolveTableMetadata(context, input_string, options);
	ret->metadata = std::move(resolved_metadata.metadata);

	ret->snapshot_to_scan = ret->metadata.GetSnapshot(*options.snapshot_lookup);

	if (ret->snapshot_to_scan.snapshot) {
		ret->iceberg_table = IcebergManifestList::Load(resolved_metadata.table_location, ret->metadata,
		                                               ret->snapshot_to_scan, context, options);
		ret->schema = ret->metadata.GetSchemaFromId(ret->snapshot_to_scan.schema_id);

		ret->source_to_column_id = ret->schema->GetSourceIdMap();
	}

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("content");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("file_path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("partition");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("column_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("column_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("lower_bound");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("upper_bound");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("column_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("value_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("null_value_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("nan_value_count");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(ret);
}

static void AddString(Vector &vec, idx_t index, string_t &&str) {
	FlatVector::GetDataMutable<string_t>(vec)[index] = StringVector::AddString(vec, std::move(str));
}

static void IcebergColumnStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergColumnStatsBindData>();
	auto &global_state = data.global_state->Cast<IcebergColumnStatsGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}

	idx_t out = 0;
	auto &schema = bind_data.schema->columns;
	auto &table_entries = bind_data.iceberg_table->GetManifestFilesConst();
	for (; global_state.current_manifest_idx < table_entries.size(); global_state.current_manifest_idx++) {
		auto &table_entry = table_entries[global_state.current_manifest_idx];
		auto &entries = table_entry.GetManifestEntries();
		for (; global_state.current_manifest_entry_idx < entries.size(); global_state.current_manifest_entry_idx++) {
			auto &manifest_entry = entries[global_state.current_manifest_entry_idx];
			auto &data_file = manifest_entry.data_file;
			child_list_t<Value> partition_fields;
			for (idx_t partition_idx = 0; partition_idx < data_file.partition_info.size(); partition_idx++) {
				auto &entry = data_file.partition_info[partition_idx];
				partition_fields.emplace_back(StringUtil::Format("r%d", partition_idx), entry.value);
			}
			auto partition_value = Value::STRUCT(std::move(partition_fields));

			for (; global_state.column_it != bind_data.source_to_column_id.end(); global_state.column_it++) {
				if (out >= STANDARD_VECTOR_SIZE) {
					output.SetChildCardinality(out);
					return;
				}
				idx_t col = 0;
				//! status
				AddString(output.data[col++], out,
				          string_t(IcebergManifestEntryStatusTypeToString(manifest_entry.status)));
				//! content
				AddString(output.data[col++], out,
				          string_t(IcebergManifestEntryContentTypeToString(data_file.content)));
				//! file_path
				AddString(output.data[col++], out, string_t(data_file.file_path));
				//! partition
				output.data[col++].SetValue(out, partition_value);

				auto &entry = global_state.column_it;
				auto &source_id = entry->first;
				auto &column_id = entry->second;

				auto &column = IcebergTableSchema::GetFromColumnIndex(schema, column_id, 0);

				Value lower_bound;
				Value upper_bound;
				Value column_size;
				Value value_count;
				Value null_value_count;
				Value nan_value_count;
				auto lower_bound_it = data_file.lower_bounds.find(source_id);
				if (lower_bound_it != data_file.lower_bounds.end()) {
					lower_bound = lower_bound_it->second;
				}
				auto upper_bound_it = data_file.upper_bounds.find(source_id);
				if (upper_bound_it != data_file.upper_bounds.end()) {
					upper_bound = upper_bound_it->second;
				}
				auto column_size_it = data_file.column_sizes.find(source_id);
				if (column_size_it != data_file.column_sizes.end()) {
					column_size = Value::BIGINT(column_size_it->second);
				}
				auto value_count_it = data_file.value_counts.find(source_id);
				if (value_count_it != data_file.value_counts.end()) {
					value_count = Value::BIGINT(value_count_it->second);
				}
				auto null_value_count_it = data_file.null_value_counts.find(source_id);
				if (null_value_count_it != data_file.null_value_counts.end()) {
					null_value_count = Value::BIGINT(null_value_count_it->second);
				}
				auto nan_value_count_it = data_file.nan_value_counts.find(source_id);
				if (nan_value_count_it != data_file.nan_value_counts.end()) {
					nan_value_count = Value::BIGINT(nan_value_count_it->second);
				}

				//! column_name
				AddString(output.data[col++], out, string_t(column.name));
				//! column_type
				AddString(output.data[col++], out, string_t(column.type.ToString()));

				optional<string> lower_bound_str;
				optional<string> upper_bound_str;
				if (column.type.id() == LogicalTypeId::VARIANT) {
					//! VARIANT lower/upper bounds are stored as a binary Variant value (an object keyed by JSON
					//! path); decode them into a readable VARIANT instead of attempting a scalar deserialization.
					Value decoded;
					if (!lower_bound.IsNull() && IcebergVariantBoundsReader::Deserialize(
					                                 context, lower_bound.GetValueUnsafe<string_t>(), decoded)) {
						lower_bound_str = decoded.ToString();
					} else {
						lower_bound_str = Value().ToString();
					}
					if (!upper_bound.IsNull() && IcebergVariantBoundsReader::Deserialize(
					                                 context, upper_bound.GetValueUnsafe<string_t>(), decoded)) {
						upper_bound_str = decoded.ToString();
					} else {
						upper_bound_str = Value().ToString();
					}
				} else {
					auto stats =
					    IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.name, column.type);
					//! GEOMETRY bounds are a bounding box (no scalar min/max), so lower_bound /
					//! upper_bound carry the box serialized as a JSON object instead of a scalar.
					bool is_geometry = column.type.id() == LogicalTypeId::GEOMETRY && stats.geometry_stats;
					if (is_geometry) {
						auto &extent = GeometryStats::GetExtent(*stats.geometry_stats);
						lower_bound_str = GeometryBoundJson(extent, true);
						upper_bound_str = GeometryBoundJson(extent, false);
					} else {
						if (stats.lower_bound) {
							lower_bound_str = stats.lower_bound->ToString();
						}
						if (stats.upper_bound) {
							upper_bound_str = stats.upper_bound->ToString();
						}
					}
				}

				//! lower_bound
				if (lower_bound_str) {
					AddString(output.data[col++], out, string_t(*lower_bound_str));
				} else {
					output.data[col++].SetValue(out, Value(LogicalType::VARCHAR));
				}

				//! upper_bound
				if (upper_bound_str) {
					AddString(output.data[col++], out, string_t(*upper_bound_str));
				} else {
					output.data[col++].SetValue(out, Value(LogicalType::VARCHAR));
				}
				// column_size
				output.data[col++].SetValue(out, column_size);
				// value_count
				output.data[col++].SetValue(out, value_count);
				// null_value_count
				output.data[col++].SetValue(out, null_value_count);
				// nan_value_count
				output.data[col++].SetValue(out, nan_value_count);
				out++;
			}
			global_state.column_it = bind_data.source_to_column_id.begin();
		}
		global_state.current_manifest_entry_idx = 0;
	}
	output.SetChildCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergColumnStatsFunction() {
	TableFunctionSet function_set("iceberg_column_stats");
	TableFunction fun({LogicalType::VARCHAR}, IcebergColumnStatsFunction, IcebergColumnStatsBind,
	                  IcebergColumnStatsGlobalTableFunctionState::Init);

	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP_MS;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
	function_set.AddFunction(fun);
	return function_set;
}

} // namespace duckdb
