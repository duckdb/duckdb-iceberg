#include "duckdb/main/client_context.hpp"

#include "function/iceberg_functions.hpp"
#include "iceberg_options.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

namespace duckdb {

//! The current snapshot and all of its parents
static unordered_set<int64_t> GetCurrentAncestors(const IcebergTableMetadata &metadata) {
	unordered_set<int64_t> result;
	if (!metadata.current_snapshot_id) {
		return result;
	}
	auto snapshot_id = *metadata.current_snapshot_id;
	while (result.insert(snapshot_id).second) {
		auto snapshot = metadata.snapshots.find(snapshot_id);
		if (snapshot == metadata.snapshots.end() || !snapshot->second.parent_snapshot_id) {
			break;
		}
		snapshot_id = *snapshot->second.parent_snapshot_id;
	}
	return result;
}

struct IcebergHistoryBindData : public TableFunctionData {
	IcebergHistoryBindData() {};
	IcebergTableMetadata metadata {IcebergTableMetadataSchemas {}};
};

struct IcebergHistoryGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<IcebergHistoryBindData>();
		auto global_state = make_uniq<IcebergHistoryGlobalState>();
		global_state->current_ancestors = GetCurrentAncestors(bind_data.metadata);
		return std::move(global_state);
	}

	unordered_set<int64_t> current_ancestors;
	//! Position in the snapshot log
	idx_t offset = 0;
};

static unique_ptr<FunctionData> IcebergHistoryBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	IcebergOptions options(input.named_parameters);
	auto input_string = input.inputs[0].ToString();
	auto bind_data = make_uniq<IcebergHistoryBindData>();
	bind_data->metadata = std::move(IcebergUtils::ResolveTableMetadata(context, input_string, options).metadata);

	names.emplace_back("made_current_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_MS);

	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("parent_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("is_current_ancestor");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(bind_data);
}

static void IcebergHistoryFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergHistoryBindData>();
	auto &global_state = data.global_state->Cast<IcebergHistoryGlobalState>();

	auto &metadata = bind_data.metadata;
	auto &snapshot_log = metadata.snapshot_log;

	idx_t count = 0;
	while (global_state.offset < snapshot_log.size() && count < STANDARD_VECTOR_SIZE) {
		auto &log_entry = snapshot_log[global_state.offset++];
		auto snapshot_id = log_entry.first;
		FlatVector::GetDataMutable<timestamp_ms_t>(output.data[0])[count] = log_entry.second;
		FlatVector::GetDataMutable<int64_t>(output.data[1])[count] = snapshot_id;
		//! The snapshot may have been expired, in which case its parent is unknown
		auto snapshot = metadata.snapshots.find(snapshot_id);
		if (snapshot != metadata.snapshots.end() && snapshot->second.parent_snapshot_id) {
			FlatVector::GetDataMutable<int64_t>(output.data[2])[count] = *snapshot->second.parent_snapshot_id;
		} else {
			FlatVector::SetNull(output.data[2], count, true);
		}
		FlatVector::GetDataMutable<bool>(output.data[3])[count] = global_state.current_ancestors.count(snapshot_id) > 0;
		count++;
	}
	output.SetChildCardinality(count);
}

TableFunctionSet IcebergFunctions::GetIcebergHistoryFunction() {
	TableFunctionSet function_set("iceberg_history");
	TableFunction table_function(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                             IcebergHistoryFunction, IcebergHistoryBind, IcebergHistoryGlobalState::Init);
	table_function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("metadata_compression_codec", LogicalType::VARCHAR)
		    .Add("version", LogicalType::ANY)
		    .Add("version_name_format", LogicalType::VARCHAR);
	});
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
