#include "duckdb/common/string.hpp"

#include "function/iceberg_functions.hpp"
#include "core/metadata/partition/iceberg_partition_struct.hpp"
#include "common/iceberg_utils.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"
#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"

namespace duckdb {

//! A primitive column of the schema
struct IcebergFilesMetricsField {
	string name;
	reference<const IcebergColumnDefinition> column;
};

struct IcebergFilesBindData : public TableFunctionData {
	IcebergFilesBindData() {};
	IcebergTableMetadata metadata {IcebergTableMetadataSchemas {}};

	IcebergOptions options;
	string table_location;
	IcebergSnapshotScanInfo snapshot_to_scan;
	vector<IcebergPartitionStructField> partition_fields;
	vector<IcebergFilesMetricsField> metrics_fields;
};

struct IcebergFilesGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<IcebergFilesBindData>();
		auto global_state = make_uniq<IcebergFilesGlobalState>();
		if (bind_data.snapshot_to_scan.snapshot) {
			global_state->manifest_list = IcebergManifestList::Load(
			    bind_data.table_location, bind_data.metadata, bind_data.snapshot_to_scan, context, bind_data.options);
		}
		return std::move(global_state);
	}

	unique_ptr<IcebergManifestList> manifest_list;
	idx_t manifest_idx = 0;
	idx_t entry_idx = 0;
	//! Assigns inherited first row ids to the entries of the current manifest
	optional<BoundIcebergManifestListEntry> bound_manifest;
};

static LogicalType ColumnMetricsType(const LogicalType &type) {
	child_list_t<LogicalType> children;
	children.emplace_back("column_size", LogicalType::BIGINT);
	children.emplace_back("value_count", LogicalType::BIGINT);
	children.emplace_back("null_value_count", LogicalType::BIGINT);
	children.emplace_back("nan_value_count", LogicalType::BIGINT);
	children.emplace_back("lower_bound", type);
	children.emplace_back("upper_bound", type);
	return LogicalType::STRUCT(std::move(children));
}

static LogicalType ReadableMetricsType(const vector<IcebergFilesMetricsField> &metrics_fields) {
	child_list_t<LogicalType> children;
	for (auto &field : metrics_fields) {
		children.emplace_back(field.name, ColumnMetricsType(field.column.get().type));
	}
	return LogicalType::STRUCT(std::move(children));
}

//! Metrics for all primitive columns, including nested
static void GetMetricsFields(const IcebergColumnDefinition &column, const string &prefix,
                             vector<IcebergFilesMetricsField> &result) {
	auto name = prefix.empty() ? column.name : prefix + "." + column.name;
	switch (column.type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		for (idx_t i = 0; i < column.GetChildCount(); i++) {
			GetMetricsFields(*column.GetChild(i), name, result);
		}
		return;
	case LogicalTypeId::VARIANT:
		//! Not a primitive type
		return;
	default:
		result.push_back(IcebergFilesMetricsField {std::move(name), column});
	}
}

static unique_ptr<FunctionData> IcebergFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<IcebergFilesBindData>();

	bind_data->options = IcebergOptions(input.named_parameters);
	auto input_string = input.inputs[0].ToString();
	auto resolved_metadata = IcebergUtils::ResolveTableMetadata(context, input_string, bind_data->options);
	bind_data->table_location = std::move(resolved_metadata.table_location);
	bind_data->metadata = std::move(resolved_metadata.metadata);
	bind_data->snapshot_to_scan = bind_data->metadata.GetSnapshot(*bind_data->options.snapshot_lookup);
	bind_data->partition_fields = IcebergPartitionStruct::GetFields(bind_data->metadata);
	for (auto &column : bind_data->metadata.GetLatestSchema().columns) {
		GetMetricsFields(*column, "", bind_data->metrics_fields);
	}
	//! Sorted by name, as Iceberg does
	std::sort(bind_data->metrics_fields.begin(), bind_data->metrics_fields.end(),
	          [](const IcebergFilesMetricsField &a, const IcebergFilesMetricsField &b) { return a.name < b.name; });

	auto count_map = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT);
	auto bounds_map = LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB);

	names.emplace_back("content");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("file_path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("file_format");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("spec_id");
	return_types.emplace_back(LogicalType::INTEGER);

	if (!bind_data->partition_fields.empty()) {
		names.emplace_back("partition");
		return_types.emplace_back(IcebergPartitionStruct::GetType(bind_data->partition_fields));
	}

	names.emplace_back("record_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("file_size_in_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("column_sizes");
	return_types.emplace_back(count_map);

	names.emplace_back("value_counts");
	return_types.emplace_back(count_map);

	names.emplace_back("null_value_counts");
	return_types.emplace_back(count_map);

	names.emplace_back("nan_value_counts");
	return_types.emplace_back(count_map);

	names.emplace_back("lower_bounds");
	return_types.emplace_back(bounds_map);

	names.emplace_back("upper_bounds");
	return_types.emplace_back(bounds_map);

	names.emplace_back("key_metadata");
	return_types.emplace_back(LogicalType::BLOB);

	names.emplace_back("split_offsets");
	return_types.emplace_back(LogicalType::LIST(LogicalType::BIGINT));

	names.emplace_back("equality_ids");
	return_types.emplace_back(LogicalType::LIST(LogicalType::INTEGER));

	names.emplace_back("sort_order_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("first_row_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("referenced_data_file");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("content_offset");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("content_size_in_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("readable_metrics");
	return_types.emplace_back(ReadableMetricsType(bind_data->metrics_fields));

	return std::move(bind_data);
}

//! Map of column id to value, NULL if the file has none
template <class T>
static Value FieldIdMap(const unordered_map<int32_t, T> &entries, const LogicalType &value_type) {
	if (entries.empty()) {
		return Value(LogicalType::MAP(LogicalType::INTEGER, value_type));
	}
	//! Ordered by column id
	map<int32_t, Value> sorted;
	for (auto &entry : entries) {
		sorted.emplace(entry.first, Value(entry.second).DefaultCastAs(value_type));
	}
	vector<Value> keys;
	vector<Value> values;
	for (auto &entry : sorted) {
		keys.push_back(Value::INTEGER(entry.first));
		values.push_back(entry.second);
	}
	return Value::MAP(LogicalType::INTEGER, value_type, std::move(keys), std::move(values));
}

//! List of values, NULL if empty
template <class T>
static Value ValueList(const vector<T> &entries, const LogicalType &type) {
	if (entries.empty()) {
		return Value(LogicalType::LIST(type));
	}
	vector<Value> values;
	for (auto &entry : entries) {
		values.push_back(Value(entry).DefaultCastAs(type));
	}
	return Value::LIST(type, std::move(values));
}

template <class T>
static Value OptionalBigint(const optional<T> &value) {
	return value ? Value::BIGINT(*value) : Value(LogicalType::BIGINT);
}

template <class T>
static Value MetricValue(const unordered_map<int32_t, T> &metrics, int32_t field_id) {
	auto it = metrics.find(field_id);
	return it == metrics.end() ? Value(LogicalType::BIGINT) : Value::BIGINT(it->second);
}

static Value BoundValue(const unordered_map<int32_t, Value> &bounds, int32_t field_id) {
	auto it = bounds.find(field_id);
	return it == bounds.end() ? Value(LogicalType::BLOB) : it->second;
}

//! The file's metrics per column, with the bounds decoded to the column type
static Value ReadableMetricsValue(ClientContext &context, const vector<IcebergFilesMetricsField> &metrics_fields,
                                  const IcebergDataFile &data_file) {
	child_list_t<Value> columns;
	for (auto &field : metrics_fields) {
		auto &column = field.column.get();
		auto &type = column.type;
		//! Bounds that can't be decoded, such as geometry bounding boxes, are left NULL
		auto lower = BoundValue(data_file.lower_bounds, column.id);
		auto upper = BoundValue(data_file.upper_bounds, column.id);
		auto stats = IcebergPredicateStats::DeserializeBounds(context, lower, upper, field.name, type);
		auto lower_bound = stats.lower_bound ? stats.lower_bound->DefaultCastAs(type) : Value(type);
		auto upper_bound = stats.upper_bound ? stats.upper_bound->DefaultCastAs(type) : Value(type);

		child_list_t<Value> metrics;
		metrics.emplace_back("column_size", MetricValue(data_file.column_sizes, column.id));
		metrics.emplace_back("value_count", MetricValue(data_file.value_counts, column.id));
		metrics.emplace_back("null_value_count", MetricValue(data_file.null_value_counts, column.id));
		metrics.emplace_back("nan_value_count", MetricValue(data_file.nan_value_counts, column.id));
		metrics.emplace_back("lower_bound", std::move(lower_bound));
		metrics.emplace_back("upper_bound", std::move(upper_bound));
		columns.emplace_back(field.name, Value::STRUCT(std::move(metrics)));
	}
	return Value::STRUCT(std::move(columns));
}

static void IcebergFilesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergFilesBindData>();
	auto &global_state = data.global_state->Cast<IcebergFilesGlobalState>();

	if (!global_state.manifest_list) {
		//! The table has no snapshot
		return;
	}

	auto &manifests = global_state.manifest_list->GetManifestFilesConst();
	idx_t out = 0;
	while (global_state.manifest_idx < manifests.size() && out < STANDARD_VECTOR_SIZE) {
		auto &manifest = manifests[global_state.manifest_idx];
		auto &manifest_file = manifest.GetFile();
		auto &entries = manifest.GetManifestEntries();
		if (global_state.entry_idx >= entries.size()) {
			global_state.manifest_idx++;
			global_state.entry_idx = 0;
			continue;
		}
		if (global_state.entry_idx == 0) {
			global_state.bound_manifest.emplace(global_state.manifest_idx, manifest);
		}
		auto &entry = entries[global_state.entry_idx++];
		//! Only files that are live in this snapshot
		if (entry.status == IcebergManifestEntryStatusType::DELETED) {
			continue;
		}
		auto &data_file = entry.data_file;

		//! A file without a first row id inherits the next one of its manifest
		auto bound_entry = global_state.bound_manifest->BindEntry(entry);
		auto first_row_id =
		    bound_entry.HasFirstRowId() ? Value::BIGINT(bound_entry.GetFirstRowId()) : Value(LogicalType::BIGINT);

		idx_t col = 0;
		output.data[col++].SetValue(out, Value::INTEGER(static_cast<int32_t>(data_file.content)));
		output.data[col++].SetValue(out, Value(data_file.file_path));
		output.data[col++].SetValue(out, Value(data_file.file_format));
		output.data[col++].SetValue(out, Value::INTEGER(manifest_file.partition_spec_id));
		if (!bind_data.partition_fields.empty()) {
			output.data[col++].SetValue(out, IcebergPartitionStruct::GetValue(bind_data.partition_fields, data_file));
		}
		output.data[col++].SetValue(out, Value::BIGINT(data_file.record_count));
		output.data[col++].SetValue(out, Value::BIGINT(data_file.file_size_in_bytes));
		output.data[col++].SetValue(out, FieldIdMap(data_file.column_sizes, LogicalType::BIGINT));
		output.data[col++].SetValue(out, FieldIdMap(data_file.value_counts, LogicalType::BIGINT));
		output.data[col++].SetValue(out, FieldIdMap(data_file.null_value_counts, LogicalType::BIGINT));
		output.data[col++].SetValue(out, FieldIdMap(data_file.nan_value_counts, LogicalType::BIGINT));
		//! Raw serialized bounds, as Iceberg stores them
		output.data[col++].SetValue(out, FieldIdMap(data_file.lower_bounds, LogicalType::BLOB));
		output.data[col++].SetValue(out, FieldIdMap(data_file.upper_bounds, LogicalType::BLOB));

		//! TODO: read it from the manifest once encrypted tables are supported
		output.data[col++].SetValue(out, Value(LogicalType::BLOB));

		output.data[col++].SetValue(out, ValueList(data_file.split_offsets, LogicalType::BIGINT));
		output.data[col++].SetValue(out, ValueList(data_file.equality_ids, LogicalType::INTEGER));
		output.data[col++].SetValue(out, data_file.sort_order_id ? Value::INTEGER(*data_file.sort_order_id)
		                                                         : Value(LogicalType::INTEGER));
		output.data[col++].SetValue(out, first_row_id);
		output.data[col++].SetValue(out, data_file.referenced_data_file ? Value(*data_file.referenced_data_file)
		                                                                : Value(LogicalType::VARCHAR));
		output.data[col++].SetValue(out, OptionalBigint(data_file.content_offset));
		output.data[col++].SetValue(out, OptionalBigint(data_file.content_size_in_bytes));
		output.data[col++].SetValue(out, ReadableMetricsValue(context, bind_data.metrics_fields, data_file));
		out++;
	}
	output.SetChildCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergFilesFunction() {
	TableFunctionSet function_set("iceberg_files");
	TableFunction table_function(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                             IcebergFilesFunction, IcebergFilesBind, IcebergFilesGlobalState::Init);

	table_function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("allow_moved_paths", LogicalType::BOOLEAN)
		    .Add("metadata_compression_codec", LogicalType::VARCHAR)
		    .Add("version", LogicalType::ANY)
		    .Add("version_name_format", LogicalType::VARCHAR)
		    .Add("snapshot_from_timestamp", LogicalType::ANY)
		    .Add("snapshot_from_id", LogicalType::UBIGINT);
	});
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
