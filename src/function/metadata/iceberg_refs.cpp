#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/storage/caching_file_system_wrapper.hpp"
#include "duckdb/common/string_util.hpp"

#include "function/iceberg_functions.hpp"
#include "iceberg_options.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

struct IcebergRefEntry {
	string name;
	rest_api_objects::SnapshotReference ref;
};

//! The table's refs
static vector<IcebergRefEntry> GetRefs(const IcebergTableMetadata &metadata) {
	vector<IcebergRefEntry> result;
	if (metadata.refs) {
		for (auto &entry : metadata.refs->additional_properties) {
			result.push_back(IcebergRefEntry {entry.first, entry.second.Copy()});
		}
	} else if (metadata.has_current_snapshot) {
		//! Older metadata has no refs, so 'main' points at the current snapshot
		rest_api_objects::SnapshotReference main_ref;
		main_ref.type = "branch";
		main_ref.snapshot_id = metadata.current_snapshot_id;
		result.push_back(IcebergRefEntry {"main", std::move(main_ref)});
	}
	return result;
}

struct IcebergRefsBindData : public TableFunctionData {
	IcebergRefsBindData() {};
	string filename;
	IcebergOptions options;
};

struct IcebergRefsGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<IcebergRefsBindData>();
		auto global_state = make_uniq<IcebergRefsGlobalState>();
		auto &fs = FileSystem::GetFileSystem(context);
		auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(fs, *context.db);
		auto path = IcebergTableMetadata::GetMetaDataPath(context, bind_data.filename, fs, bind_data.options);
		auto table_metadata =
		    IcebergTableMetadata::Parse(path, *caching_fs, bind_data.options.metadata_compression_codec);
		auto metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);
		global_state->refs = GetRefs(metadata);
		return std::move(global_state);
	}

	vector<IcebergRefEntry> refs;
	//! Position in the list of refs
	idx_t offset = 0;
};

static unique_ptr<FunctionData> IcebergRefsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<IcebergRefsBindData>();
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
				    "'version_name_format' has to contain two occurrences of '%%s' in it, found %d",
				    string_substitutions);
			}
			bind_data->options.version_name_format = value;
		}
	}
	auto input_string = input.inputs[0].ToString();
	bind_data->filename = IcebergUtils::GetStorageLocation(context, input_string);

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("max_reference_age_in_ms");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("min_snapshots_to_keep");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("max_snapshot_age_in_ms");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(bind_data);
}

static void IcebergRefsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<IcebergRefsGlobalState>();

	idx_t out = 0;
	while (global_state.offset < global_state.refs.size() && out < STANDARD_VECTOR_SIZE) {
		auto &entry = global_state.refs[global_state.offset++];
		auto &ref = entry.ref;

		idx_t col = 0;
		output.data[col++].SetValue(out, Value(entry.name));
		//! Upper case, matching Iceberg's BRANCH and TAG
		output.data[col++].SetValue(out, Value(StringUtil::Upper(ref.type)));
		output.data[col++].SetValue(out, Value::BIGINT(ref.snapshot_id));
		output.data[col++].SetValue(out, ref.has_max_ref_age_ms ? Value::BIGINT(ref.max_ref_age_ms)
		                                                        : Value(LogicalType::BIGINT));
		output.data[col++].SetValue(out, ref.has_min_snapshots_to_keep ? Value::INTEGER(ref.min_snapshots_to_keep)
		                                                               : Value(LogicalType::INTEGER));
		output.data[col++].SetValue(out, ref.has_max_snapshot_age_ms ? Value::BIGINT(ref.max_snapshot_age_ms)
		                                                             : Value(LogicalType::BIGINT));
		out++;
	}
	output.SetCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergRefsFunction() {
	TableFunctionSet function_set("iceberg_refs");
	TableFunction table_function({LogicalType::VARCHAR}, IcebergRefsFunction, IcebergRefsBind,
	                             IcebergRefsGlobalState::Init);
	table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	table_function.named_parameters["version"] = LogicalType::VARCHAR;
	table_function.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
