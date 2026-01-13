#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_functions.hpp"
#include "iceberg_utils.hpp"

#include "metadata/iceberg_table_metadata.hpp"

#include <string>
#include <numeric>

namespace duckdb {

struct IcebergPartitionStatsBindData : public TableFunctionData {
	optional_ptr<IcebergSnapshot> snapshot_to_scan;
	IcebergTableMetadata metadata;
	shared_ptr<IcebergTableSchema> schema;
	unordered_map<uint64_t, ColumnIndex> source_to_column_id;
	unique_ptr<IcebergTable> iceberg_table;
};

struct IcebergPartitionStatsGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	IcebergPartitionStatsGlobalTableFunctionState() {

	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergPartitionStatsGlobalTableFunctionState>();
	}

	idx_t current_manifest_idx = 0;
	idx_t current_manifest_entry_idx = 0;
};

static unique_ptr<FunctionData> IcebergPartitionStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<IcebergPartitionStatsBindData>();

	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto input_string = input.inputs[0].ToString();
	auto filename = IcebergUtils::GetStorageLocation(context, input_string);

	IcebergOptions options;
	auto &snapshot_lookup = options.snapshot_lookup;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		auto &val = kv.second;
		if (loption == "allow_moved_paths") {
			options.allow_moved_paths = BooleanValue::Get(val);
		} else if (loption == "metadata_compression_codec") {
			options.metadata_compression_codec = StringValue::Get(val);
		} else if (loption == "version") {
			options.table_version = StringValue::Get(val);
		} else if (loption == "version_name_format") {
			auto value = StringValue::Get(kv.second);
			auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
			if (string_substitutions != 2) {
				throw InvalidInputException(
				    "'version_name_format' has to contain two occurrences of '%s' in it, found %d",
				    string_substitutions);
			}
			options.version_name_format = value;
		} else if (loption == "snapshot_from_id") {
			if (snapshot_lookup.snapshot_source != SnapshotSource::LATEST) {
				throw InvalidInputException(
				    "Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
			}
			snapshot_lookup.snapshot_source = SnapshotSource::FROM_ID;
			snapshot_lookup.snapshot_id = val.GetValue<uint64_t>();
		} else if (loption == "snapshot_from_timestamp") {
			if (snapshot_lookup.snapshot_source != SnapshotSource::LATEST) {
				throw InvalidInputException(
				    "Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
			}
			snapshot_lookup.snapshot_source = SnapshotSource::FROM_TIMESTAMP;
			snapshot_lookup.snapshot_timestamp = val.GetValue<timestamp_t>();
		}
	}

	auto iceberg_meta_path = IcebergTableMetadata::GetMetaDataPath(context, filename, fs, options);
	auto table_metadata = IcebergTableMetadata::Parse(iceberg_meta_path, fs, options.metadata_compression_codec);
	ret->metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);

	ret->snapshot_to_scan = ret->metadata.GetSnapshot(options.snapshot_lookup);

	if (ret->snapshot_to_scan) {
		ret->iceberg_table = IcebergTable::Load(filename, ret->metadata, *ret->snapshot_to_scan, context, options);
		ret->schema = ret->metadata.GetSchemaFromId(ret->snapshot_to_scan->schema_id);

		auto &schema = ret->schema->columns;
		IcebergTableSchema::PopulateSourceIdMap(ret->source_to_column_id, schema, nullptr);
	}

	names.emplace_back("manifest_path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("added_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("partition_spec_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("partition_field_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("partition_field_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("partition_source_columns");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("partition_field_transform");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("partition_field_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("lower_bound");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("upper_bound");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("contains_null");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("contains_nan");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(ret);
}

static void AddString(Vector &vec, idx_t index, string_t &&str) {
	FlatVector::GetData<string_t>(vec)[index] = StringVector::AddString(vec, std::move(str));
}

static void IcebergPartitionStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergPartitionStatsBindData>();
	auto &global_state = data.global_state->Cast<IcebergPartitionStatsGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}

	idx_t out = 0;
	auto &schema = bind_data.schema->columns;
	auto &table_entries = bind_data.iceberg_table->entries;
	auto &metadata = bind_data.metadata;
	for (; global_state.current_manifest_idx < table_entries.size(); global_state.current_manifest_idx++) {
		auto &table_entry = table_entries[global_state.current_manifest_idx];
		auto &manifest = table_entry.manifest;
		auto &field_summaries = manifest.partitions.field_summary;

		auto spec_id = manifest.partition_spec_id;
		auto partition_spec_it = metadata.partition_specs.find(spec_id);
		if (partition_spec_it == metadata.partition_specs.end()) {
			throw InvalidInputException("Manifest %s references 'partition_spec_id' %d which doesn't exist",
			                            manifest.manifest_path, spec_id);
		}
		auto &partition_spec = partition_spec_it->second;
		for (; global_state.current_manifest_entry_idx < field_summaries.size();
		     global_state.current_manifest_entry_idx++) {
			if (out >= STANDARD_VECTOR_SIZE) {
				output.SetCardinality(out);
				return;
			}
			auto &field_summary = field_summaries[global_state.current_manifest_entry_idx];
			auto &field = partition_spec.fields[global_state.current_manifest_entry_idx];

			const auto &column_id = bind_data.source_to_column_id.at(field.source_id);
			auto &column = IcebergTableSchema::GetFromColumnIndex(schema, column_id, 0);
			auto result_type = field.transform.GetSerializedType(column.type);

			idx_t col = 0;
			//! manifest_path
			AddString(output.data[col++], out, string_t(manifest.manifest_path));
			//! added_snapshot_id
			FlatVector::GetData<int64_t>(output.data[col++])[out] = manifest.added_snapshot_id;
			//! partition_spec_id
			FlatVector::GetData<int32_t>(output.data[col++])[out] = manifest.partition_spec_id;
			//! partition_field_id
			FlatVector::GetData<uint64_t>(output.data[col++])[out] = field.partition_field_id;
			//! partition_field_name
			AddString(output.data[col++], out, string_t(field.name));
			//! partition_source_columns
			output.data[col++].SetValue(out, Value::LIST({column.name}));
			//! partition_field_transform
			AddString(output.data[col++], out, string_t(field.transform.RawType()));

			auto stats = IcebergPredicateStats::DeserializeBounds(field_summary.lower_bound, field_summary.upper_bound,
			                                                      column.name, result_type);
			//! partition_field_type
			AddString(output.data[col++], out, string_t(result_type.ToString()));

			//! lower_bound
			AddString(output.data[col++], out, string_t(stats.lower_bound.ToString()));
			//! upper_bound
			AddString(output.data[col++], out, string_t(stats.upper_bound.ToString()));

			//! contains_null
			FlatVector::GetData<bool>(output.data[col++])[out] = field_summary.contains_null;
			//! contains_nan
			FlatVector::GetData<bool>(output.data[col++])[out] = field_summary.contains_nan;

			out++;
		}
		global_state.current_manifest_entry_idx = 0;
	}
	output.SetCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergPartitionStatsFunction() {
	TableFunctionSet function_set("iceberg_partition_stats");
	TableFunction fun({LogicalType::VARCHAR}, IcebergPartitionStatsFunction, IcebergPartitionStatsBind,
	                  IcebergPartitionStatsGlobalTableFunctionState::Init);

	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
	function_set.AddFunction(fun);
	return function_set;
}

} // namespace duckdb
