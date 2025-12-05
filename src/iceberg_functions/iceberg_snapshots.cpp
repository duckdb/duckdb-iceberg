#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_functions.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_options.hpp"
#include "iceberg_utils.hpp"
#include "metadata/iceberg_table_metadata.hpp"

#include <string>

namespace duckdb {

struct IcebergSnaphotsBindData : public TableFunctionData {
	IcebergSnaphotsBindData() {};
	string filename;
	IcebergOptions options;
};

struct IcebergSnapshotGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto bind_data = input.bind_data->Cast<IcebergSnaphotsBindData>();
		auto global_state = make_uniq<IcebergSnapshotGlobalTableFunctionState>();

		FileSystem &fs = FileSystem::GetFileSystem(context);

		auto iceberg_meta_path =
		    IcebergTableMetadata::GetMetaDataPath(context, bind_data.filename, fs, bind_data.options);
		auto table_metadata =
		    IcebergTableMetadata::Parse(iceberg_meta_path, fs, bind_data.options.metadata_compression_codec);
		global_state->metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);

		auto &info = global_state->metadata;
		global_state->snapshot_it = info.snapshots.begin();
		return std::move(global_state);
	}

	IcebergTableMetadata metadata;
	unordered_map<int64_t, IcebergSnapshot>::iterator snapshot_it;
};

static unique_ptr<FunctionData> IcebergSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<IcebergSnaphotsBindData>();

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "metadata_compression_codec") {
			bind_data->options.metadata_compression_codec = StringValue::Get(kv.second);
		} else if (loption == "version") {
			bind_data->options.table_version = StringValue::Get(kv.second);
		} else if (loption == "version_name_format") {
			auto value = StringValue::Get(kv.second);
			auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
			if (string_substitutions != 2) {
				throw InvalidInputException(
				    "'version_name_format' has to contain two occurrences of '%s' in it, found %d", "%s",
				    string_substitutions);
			}
			bind_data->options.version_name_format = value;
		}
	}
	auto input_string = input.inputs[0].ToString();
	bind_data->filename = IcebergUtils::GetStorageLocation(context, input_string);

	names.emplace_back("sequence_number");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("timestamp_ms");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	names.emplace_back("manifest_list");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(bind_data);
}

// Snapshots function
static void IcebergSnapshotsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<IcebergSnapshotGlobalTableFunctionState>();
	idx_t i = 0;
	auto &it = global_state.snapshot_it;
	auto end = global_state.metadata.snapshots.end();
	for (; it != end; it++) {
		if (i >= STANDARD_VECTOR_SIZE) {
			break;
		}

		auto &snapshot = it->second;
		FlatVector::GetData<uint64_t>(output.data[0])[i] = snapshot.sequence_number;
		FlatVector::GetData<uint64_t>(output.data[1])[i] = snapshot.snapshot_id;
		FlatVector::GetData<timestamp_t>(output.data[2])[i] = snapshot.timestamp_ms;
		string_t manifest_string_t = StringVector::AddString(output.data[3], string_t(snapshot.manifest_list));
		FlatVector::GetData<string_t>(output.data[3])[i] = manifest_string_t;
		i++;
	}
	output.SetCardinality(i);
}

TableFunctionSet IcebergFunctions::GetIcebergSnapshotsFunction() {
	TableFunctionSet function_set("iceberg_snapshots");
	TableFunction table_function({LogicalType::VARCHAR}, IcebergSnapshotsFunction, IcebergSnapshotsBind,
	                             IcebergSnapshotGlobalTableFunctionState::Init);
	table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	table_function.named_parameters["version"] = LogicalType::VARCHAR;
	table_function.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
