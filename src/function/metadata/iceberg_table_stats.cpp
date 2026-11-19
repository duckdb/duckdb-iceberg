#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/puffin/iceberg_puffin_metadata.hpp"
#include "function/iceberg_functions.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

namespace {

//! One row per blob of a table statistics file
struct IcebergTableStatsEntry {
	int64_t statistics_snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
	int64_t file_footer_size_in_bytes;
	string blob_type;
	int64_t blob_snapshot_id;
	int64_t blob_sequence_number;
	Value field_ids;
	Value field_names;
	Value ndv;
	Value properties;
};

struct IcebergTableStatsBindData : public TableFunctionData {
	vector<IcebergTableStatsEntry> entries;
};

struct IcebergTableStatsGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergTableStatsGlobalState>();
	}

	idx_t offset = 0;
};

//! A path without a scheme and not starting at the root is relative to the table location; a full path is moved
//! under the table location only with allow_moved_paths
static string ResolveStatisticsPath(const string &table_location, const IcebergOptions &options, FileSystem &fs,
                                    const string &path) {
	const bool full_path =
	    FileSystem::IsRemoteFile(path) || StringUtil::StartsWith(path, "/") || fs.IsPathAbsolute(path);
	if (!full_path) {
		return fs.JoinPath(table_location, path);
	}
	if (options.allow_moved_paths) {
		return IcebergUtils::GetFullPath(table_location, path, fs);
	}
	return path;
}

static Value FieldIdsValue(const vector<int32_t> &field_ids) {
	vector<Value> values;
	values.reserve(field_ids.size());
	for (auto field_id : field_ids) {
		values.emplace_back(Value::INTEGER(field_id));
	}
	return Value::LIST(LogicalType::INTEGER, std::move(values));
}

//! The full names of the blob's fields in the current schema; NULL when a field is no longer in it
static Value FieldNamesValue(const IcebergTableMetadata &metadata, const IcebergStatisticsBlob &blob) {
	auto &schema = metadata.GetLatestSchema();
	vector<Value> names;
	names.reserve(blob.fields.size());
	for (auto field_id : blob.fields) {
		if (!schema.TryGetColumnByFieldId(field_id)) {
			return Value(LogicalType::LIST(LogicalType::VARCHAR));
		}
		names.emplace_back(schema.GetFullColumnName(field_id));
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(names));
}

//! The properties in key order, so the output does not depend on the map's iteration order
static Value PropertiesValue(const unordered_map<string, string> &properties) {
	vector<string> names;
	names.reserve(properties.size());
	for (auto &entry : properties) {
		names.push_back(entry.first);
	}
	std::sort(names.begin(), names.end());
	vector<Value> keys;
	vector<Value> values;
	keys.reserve(names.size());
	values.reserve(names.size());
	for (auto &name : names) {
		keys.emplace_back(name);
		values.emplace_back(properties.at(name));
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

static bool BlobMatches(const IcebergStatisticsBlob &registered, const IcebergPuffinBlobMetadata &in_file) {
	return registered.type == in_file.type && registered.snapshot_id == in_file.snapshot_id &&
	       registered.sequence_number == in_file.sequence_number && registered.fields == in_file.fields;
}

//! The file's footer must describe the blobs the table metadata registered for it
static void VerifyStatisticsFile(FileSystem &fs, const IcebergFileSecret &file_secret,
                                 const IcebergStatisticsFile &file, const string &path) {
	auto open_file = OpenFileInfo(path);
	file_secret.Apply(open_file);
	auto handle = fs.OpenFile(open_file, FileFlags::FILE_FLAGS_READ);
	auto footer_result =
	    IcebergPuffinReader::ReadFooter(fs, *handle, path, file.file_size_in_bytes, file.file_footer_size_in_bytes);
	if (auto error = std::get_if<string>(&footer_result)) {
		throw InvalidConfigurationException("Statistics file of snapshot %lld: %s", file.snapshot_id, *error);
	}
	auto &blobs_in_file = std::get<IcebergPuffinFileFooter>(footer_result).file_metadata.blobs;
	for (auto &blob : file.blobs) {
		bool found = false;
		for (auto &in_file : blobs_in_file) {
			if (BlobMatches(blob, in_file)) {
				found = true;
				break;
			}
		}
		if (!found) {
			throw InvalidConfigurationException("Statistics file '%s' has no blob of type '%s' for snapshot %lld, "
			                                    "sequence number %lld, which the table metadata registers",
			                                    path, blob.type, blob.snapshot_id, blob.sequence_number);
		}
	}
}

static unique_ptr<FunctionData> IcebergTableStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<IcebergTableStatsBindData>();
	auto input_string = input.inputs[0].ToString();
	IcebergOptions options(input.named_parameters);
	auto resolved = IcebergUtils::ResolveTableMetadata(context, input_string, options);
	auto &metadata = *resolved.metadata;
	auto &fs = FileSystem::GetFileSystem(context);

	//! Every statistics file of the table; a snapshot lookup narrows it to the file of that snapshot
	optional<int64_t> snapshot_id;
	if (!options.snapshot_lookup->IsLatest()) {
		auto snapshot = metadata.GetSnapshot(*options.snapshot_lookup);
		if (!snapshot.snapshot) {
			return std::move(bind_data);
		}
		snapshot_id = snapshot.snapshot->snapshot_id;
	}

	for (auto &file : metadata.statistics) {
		if (snapshot_id && file.snapshot_id != *snapshot_id) {
			continue;
		}
		auto path = ResolveStatisticsPath(resolved.table_location, options, fs, file.path);
		VerifyStatisticsFile(fs, resolved.file_secret, file, path);
		for (auto &blob : file.blobs) {
			IcebergTableStatsEntry entry;
			entry.statistics_snapshot_id = file.snapshot_id;
			entry.statistics_path = path;
			entry.file_size_in_bytes = file.file_size_in_bytes;
			entry.file_footer_size_in_bytes = file.file_footer_size_in_bytes;
			entry.blob_type = blob.type;
			entry.blob_snapshot_id = blob.snapshot_id;
			entry.blob_sequence_number = blob.sequence_number;
			entry.field_ids = FieldIdsValue(blob.fields);
			entry.field_names = FieldNamesValue(metadata, blob);
			auto distinct_count = blob.GetDistinctCount();
			entry.ndv = distinct_count ? Value::BIGINT(*distinct_count) : Value(LogicalType::BIGINT);
			entry.properties = PropertiesValue(blob.properties);
			bind_data->entries.push_back(std::move(entry));
		}
	}
	return std::move(bind_data);
}

static void IcebergTableStatsBindTypes(vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("statistics_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("statistics_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("file_size_in_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("file_footer_size_in_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("blob_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("blob_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("blob_sequence_number");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("field_ids");
	return_types.emplace_back(LogicalType::LIST(LogicalType::INTEGER));
	names.emplace_back("field_names");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
	names.emplace_back("ndv");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("properties");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
}

static unique_ptr<FunctionData> IcebergTableStatsBindWithTypes(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<Identifier> &names) {
	auto bind_data = IcebergTableStatsBind(context, input, return_types, names);
	IcebergTableStatsBindTypes(return_types, names);
	return bind_data;
}

static void IcebergTableStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergTableStatsBindData>();
	auto &global_state = data.global_state->Cast<IcebergTableStatsGlobalState>();

	idx_t out = 0;
	while (global_state.offset < bind_data.entries.size() && out < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.entries[global_state.offset++];
		idx_t col = 0;
		output.data[col++].SetValue(out, Value::BIGINT(entry.statistics_snapshot_id));
		output.data[col++].SetValue(out, Value(entry.statistics_path));
		output.data[col++].SetValue(out, Value::BIGINT(entry.file_size_in_bytes));
		output.data[col++].SetValue(out, Value::BIGINT(entry.file_footer_size_in_bytes));
		output.data[col++].SetValue(out, Value(entry.blob_type));
		output.data[col++].SetValue(out, Value::BIGINT(entry.blob_snapshot_id));
		output.data[col++].SetValue(out, Value::BIGINT(entry.blob_sequence_number));
		output.data[col++].SetValue(out, entry.field_ids);
		output.data[col++].SetValue(out, entry.field_names);
		output.data[col++].SetValue(out, entry.ndv);
		output.data[col++].SetValue(out, entry.properties);
		out++;
	}
	output.SetChildCardinality(out);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergTableStatsFunction() {
	TableFunctionSet function_set("iceberg_table_stats");
	TableFunction table_function(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                             IcebergTableStatsFunction, IcebergTableStatsBindWithTypes,
	                             IcebergTableStatsGlobalState::Init);
	table_function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("allow_moved_paths", LogicalType::BOOLEAN)
		    .Add("metadata_compression_codec", LogicalType::VARCHAR)
		    .Add("snapshot_from_id", LogicalType::UBIGINT)
		    .Add("snapshot_from_timestamp", LogicalType::TIMESTAMP)
		    .Add("version", LogicalType::ANY)
		    .Add("version_name_format", LogicalType::VARCHAR);
	});
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
