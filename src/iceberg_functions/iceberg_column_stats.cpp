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

struct IcebergColumnStatsBindData : public TableFunctionData {
	optional_ptr<IcebergSnapshot> snapshot_to_scan;
	IcebergTableMetadata metadata;
	shared_ptr<IcebergTableSchema> schema;
	unordered_map<uint64_t, ColumnIndex> source_to_column_id;
	unique_ptr<IcebergTable> iceberg_table;
};

struct IcebergColumnStatsGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	IcebergColumnStatsGlobalTableFunctionState(const IcebergColumnStatsBindData &bind_data) {
		column_it = bind_data.source_to_column_id.begin();
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergColumnStatsGlobalTableFunctionState>(input.bind_data->Cast<IcebergColumnStatsBindData>());
	}

	idx_t current_manifest_idx = 0;
	idx_t current_manifest_entry_idx = 0;
	unordered_map<uint64_t, ColumnIndex>::const_iterator column_it;
};

static unique_ptr<FunctionData> IcebergColumnStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<IcebergColumnStatsBindData>();

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
				    "'version_name_format' has to contain two occurrences of '%s' in it, found %d", "%s",
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

    names.emplace_back("status");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("content");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("file_path");
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
	FlatVector::GetData<string_t>(vec)[index] = StringVector::AddString(vec, std::move(str));
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
	auto &table_entries = bind_data.iceberg_table->entries;
	auto &metadata = bind_data.metadata;
	for (; global_state.current_manifest_idx < table_entries.size(); global_state.current_manifest_idx++) {
		auto &table_entry = table_entries[global_state.current_manifest_idx];
		auto &data_files = table_entry.manifest_file.data_files;
		for (; global_state.current_manifest_entry_idx < data_files.size(); global_state.current_manifest_entry_idx++) {
			auto &manifest = table_entry.manifest;
			auto &data_file = data_files[global_state.current_manifest_entry_idx];

			for (; global_state.column_it != bind_data.source_to_column_id.end(); global_state.column_it++) {
				if (out >= STANDARD_VECTOR_SIZE) {
					output.SetCardinality(out);
					return;
				}
				idx_t col = 0;
				//! status
				AddString(output.data[col++], out, string_t(IcebergManifestEntry::StatusTypeToString(data_file.status)));
				//! content
				AddString(output.data[col++], out, string_t(IcebergManifestEntry::ContentTypeToString(data_file.content)));
				//! file_path
				AddString(output.data[col++], out, string_t(data_file.file_path));

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

				auto stats = IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.name, column.type);

				//! column_name
				AddString(output.data[col++], out, string_t(column.name));
				//! column_type
				AddString(output.data[col++], out, string_t(column.type.ToString()));

				//! lower_bound
				AddString(output.data[col++], out, string_t(stats.lower_bound.ToString()));
				//! upper_bound
				AddString(output.data[col++], out, string_t(stats.upper_bound.ToString()));

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
	output.SetCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergColumnStatsFunction() {
	TableFunctionSet function_set("iceberg_column_stats");
	TableFunction fun(
		{LogicalType::VARCHAR},
		IcebergColumnStatsFunction,
		IcebergColumnStatsBind,
		IcebergColumnStatsGlobalTableFunctionState::Init
	);

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
