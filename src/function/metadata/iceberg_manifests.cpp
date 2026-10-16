#include "duckdb/common/string.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {

struct IcebergManifestsBindData : public TableFunctionData {
	IcebergManifestsBindData() {};
	IcebergTableMetadata metadata {IcebergTableMetadataSchemas {}};
	optional_ptr<const IcebergTableSchema> schema;
	unique_ptr<IcebergManifestList> manifest_list;
};

struct IcebergManifestsGlobalState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergManifestsGlobalState>();
	}

	idx_t manifest_idx = 0;
};

static LogicalType PartitionSummaryType() {
	child_list_t<LogicalType> children;
	children.emplace_back("contains_null", LogicalType::BOOLEAN);
	children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	children.emplace_back("lower_bound", LogicalType::VARCHAR);
	children.emplace_back("upper_bound", LogicalType::VARCHAR);
	return LogicalType::STRUCT(std::move(children));
}

static unique_ptr<FunctionData> IcebergManifestsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<IcebergManifestsBindData>();

	IcebergOptions options(input.named_parameters);
	auto input_string = input.inputs[0].ToString();
	auto resolved_metadata = IcebergUtils::ResolveTableMetadata(context, input_string, options);

	bind_data->metadata = std::move(resolved_metadata.metadata);
	auto snapshot_to_scan = bind_data->metadata.GetSnapshot(*options.snapshot_lookup);
	if (snapshot_to_scan.snapshot) {
		bind_data->manifest_list = IcebergManifestList::Load(resolved_metadata.table_location, bind_data->metadata,
		                                                     snapshot_to_scan, context, options);
		bind_data->schema = bind_data->metadata.GetSchemaFromId(snapshot_to_scan.schema_id);
	}

	names.emplace_back("content");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("length");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("partition_spec_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("added_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("added_data_files_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("existing_data_files_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("deleted_data_files_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("added_delete_files_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("existing_delete_files_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("deleted_delete_files_count");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("partition_summaries");
	return_types.emplace_back(LogicalType::LIST(PartitionSummaryType()));

	return std::move(bind_data);
}

//! Returns the file count if matches the data file content type.
static Value FileCount(const optional<idx_t> &count, bool matches_content) {
	if (!matches_content) {
		return Value::INTEGER(0);
	}
	if (!count) {
		//! Counts are optional in v1 manifest lists
		return Value(LogicalType::INTEGER);
	}
	return Value::INTEGER(NumericCast<int32_t>(*count));
}

static Value BoundToString(const IcebergTransform &transform, const optional<Value> &bound) {
	if (!bound || bound->IsNull()) {
		return Value(LogicalType::VARCHAR);
	}
	return Value(transform.PartitionValueToString(*bound));
}

static Value GetPartitionSummaries(ClientContext &context, const IcebergManifestsBindData &bind_data,
                                   const IcebergManifestFile &manifest) {
	auto &metadata = bind_data.metadata;
	auto spec_it = metadata.partition_specs.find(manifest.partition_spec_id);
	if (spec_it == metadata.partition_specs.end()) {
		throw InvalidInputException("Manifest %s references 'partition_spec_id' %d which doesn't exist",
		                            manifest.manifest_path, manifest.partition_spec_id);
	}
	auto &partition_spec = spec_it->second;
	auto &source_to_column_id = bind_data.schema->GetSourceIdMap();

	vector<Value> summaries;
	auto &field_summaries = manifest.partitions.field_summary;
	for (idx_t i = 0; i < field_summaries.size() && i < partition_spec.fields.size(); i++) {
		auto &field_summary = field_summaries[i];
		auto &field = partition_spec.fields[i];

		Value lower_bound(LogicalType::VARCHAR);
		Value upper_bound(LogicalType::VARCHAR);
		//! The source column may have been dropped from the schema, then the bounds are left NULL
		auto source_it = source_to_column_id.find(field.source_id);
		if (source_it != source_to_column_id.end()) {
			auto &column = IcebergTableSchema::GetFromColumnIndex(bind_data.schema->columns, source_it->second, 0);
			auto result_type = field.transform.GetSerializedType(column.type);
			auto stats = IcebergPredicateStats::DeserializeBounds(context, field_summary.lower_bound,
			                                                      field_summary.upper_bound, column.name, result_type);
			lower_bound = BoundToString(field.transform, stats.lower_bound);
			upper_bound = BoundToString(field.transform, stats.upper_bound);
		}

		child_list_t<Value> children;
		children.emplace_back("contains_null", Value::BOOLEAN(field_summary.contains_null));
		children.emplace_back("contains_nan", field_summary.contains_nan ? Value::BOOLEAN(*field_summary.contains_nan)
		                                                                 : Value(LogicalType::BOOLEAN));
		children.emplace_back("lower_bound", std::move(lower_bound));
		children.emplace_back("upper_bound", std::move(upper_bound));
		summaries.push_back(Value::STRUCT(std::move(children)));
	}
	return Value::LIST(PartitionSummaryType(), std::move(summaries));
}

static void IcebergManifestsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergManifestsBindData>();
	auto &global_state = data.global_state->Cast<IcebergManifestsGlobalState>();

	if (!bind_data.manifest_list) {
		//! The table has no snapshot
		return;
	}

	auto &manifests = bind_data.manifest_list->GetManifestFilesConst();
	idx_t out = 0;
	while (global_state.manifest_idx < manifests.size() && out < STANDARD_VECTOR_SIZE) {
		auto &manifest = manifests[global_state.manifest_idx++].GetFile();
		bool is_data_manifest = manifest.content == IcebergManifestContentType::DATA;
		auto &counts = manifest.counts;
		optional<idx_t> added = counts ? counts->added_files_count : optional<idx_t>();
		optional<idx_t> existing = counts ? counts->existing_files_count : optional<idx_t>();
		optional<idx_t> deleted = counts ? counts->deleted_files_count : optional<idx_t>();

		idx_t col = 0;
		output.SetValue(col++, out, Value::INTEGER(static_cast<int32_t>(manifest.content)));
		output.SetValue(col++, out, Value(manifest.manifest_path));
		output.SetValue(col++, out, Value::BIGINT(manifest.manifest_length));
		output.SetValue(col++, out, Value::INTEGER(manifest.partition_spec_id));
		output.SetValue(col++, out, Value::BIGINT(manifest.added_snapshot_id));
		output.SetValue(col++, out, FileCount(added, is_data_manifest));
		output.SetValue(col++, out, FileCount(existing, is_data_manifest));
		output.SetValue(col++, out, FileCount(deleted, is_data_manifest));
		output.SetValue(col++, out, FileCount(added, !is_data_manifest));
		output.SetValue(col++, out, FileCount(existing, !is_data_manifest));
		output.SetValue(col++, out, FileCount(deleted, !is_data_manifest));
		output.SetValue(col++, out, GetPartitionSummaries(context, bind_data, manifest));
		out++;
	}
	output.SetChildCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergManifestsFunction() {
	TableFunctionSet function_set("iceberg_manifests");
	TableFunction table_function(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                             IcebergManifestsFunction, IcebergManifestsBind, IcebergManifestsGlobalState::Init);

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
