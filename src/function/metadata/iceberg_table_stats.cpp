#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/puffin/iceberg_puffin_metadata.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

namespace {

static constexpr const char *THETA_BLOB_TYPE = "apache-datasketches-theta-v1";

struct IcebergTableStatsRow {
	Value statistics_snapshot_id;
	Value statistics_path;
	Value file_size_in_bytes;
	Value file_footer_size_in_bytes;
	Value blob_index;
	Value blob_type;
	Value blob_snapshot_id;
	Value blob_sequence_number;
	Value field_ids;
	Value field_names;
	Value ndv;
	Value properties;
	Value puffin_offset;
	Value puffin_length;
	Value puffin_compression_codec;
};

struct IcebergTableStatsBindData : public TableFunctionData {
	vector<IcebergTableStatsRow> rows;
};

struct IcebergTableStatsGlobalState : public GlobalTableFunctionState {
	explicit IcebergTableStatsGlobalState(const IcebergTableStatsBindData &bind_data) : bind_data(bind_data) {
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergTableStatsGlobalState>(input.bind_data->Cast<IcebergTableStatsBindData>());
	}

	const IcebergTableStatsBindData &bind_data;
	idx_t offset = 0;
};

static void ParseTableFunctionOptions(IcebergOptions &options, const named_parameter_map_t &named_parameters) {
	auto &snapshot_lookup = options.snapshot_lookup;
	for (auto &kv : named_parameters) {
		auto loption = StringUtil::Lower(kv.first.GetIdentifierName());
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
				    "'version_name_format' has to contain two occurrences of '%%s' in it, found %d",
				    string_substitutions);
			}
			options.version_name_format = value;
		} else if (loption == "snapshot_from_id") {
			if (snapshot_lookup.GetSource() != SnapshotSource::LATEST) {
				throw InvalidInputException(
				    "Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
			}
			snapshot_lookup.SetSource(SnapshotSource::FROM_ID);
			snapshot_lookup.snapshot_id = val.GetValue<uint64_t>();
		} else if (loption == "snapshot_from_timestamp") {
			if (snapshot_lookup.GetSource() != SnapshotSource::LATEST) {
				throw InvalidInputException(
				    "Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
			}
			snapshot_lookup.SetSource(SnapshotSource::FROM_TIMESTAMP);
			snapshot_lookup.snapshot_timestamp = val.GetValue<timestamp_t>();
		}
	}
}

static string ResolveStatisticsPath(const IcebergTableMetadata &metadata, FileSystem &fs, const IcebergOptions &options,
                                    const string &statistics_path) {
	if (fs.IsPathAbsolute(statistics_path)) {
		return statistics_path;
	}
	if (options.allow_moved_paths) {
		return IcebergUtils::GetFullPath(metadata.GetLocation(), statistics_path, fs);
	}
	return fs.JoinPath(metadata.GetLocation(), statistics_path);
}

static Value PropertiesToValue(optional_ptr<const case_insensitive_map_t<string>> properties) {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	if (!properties) {
		return Value(map_type);
	}
	vector<Value> keys;
	vector<Value> values;
	keys.reserve(properties->size());
	values.reserve(properties->size());
	for (auto &entry : *properties) {
		keys.emplace_back(entry.first);
		values.emplace_back(entry.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

static Value IntListToValue(const vector<int32_t> &values) {
	vector<Value> list_values;
	list_values.reserve(values.size());
	for (auto value : values) {
		list_values.emplace_back(Value::INTEGER(value));
	}
	return Value::LIST(std::move(list_values));
}

static Value ResolveFieldNames(const IcebergTableMetadata &metadata, const vector<int32_t> &field_ids,
                               int64_t snapshot_id) {
	auto snapshot = metadata.FindSnapshotByIdInternal(snapshot_id);
	if (!snapshot) {
		return Value(LogicalType::LIST(LogicalType::VARCHAR));
	}
	auto &schemas = metadata.GetSchemas();
	auto schema_it = schemas.find(snapshot->GetSchemaId());
	if (schema_it == schemas.end()) {
		return Value(LogicalType::LIST(LogicalType::VARCHAR));
	}

	vector<Value> field_names;
	field_names.reserve(field_ids.size());
	for (auto field_id : field_ids) {
		auto column_index = schema_it->second->TryGetColumnIndexByFieldId(field_id);
		if (!column_index) {
			return Value(LogicalType::LIST(LogicalType::VARCHAR));
		}
		auto &column = IcebergTableSchema::GetFromColumnIndex(schema_it->second->columns, *column_index, 0);
		field_names.emplace_back(column.name);
	}
	return Value::LIST(std::move(field_names));
}

static Value ParseNDV(const string &blob_type, optional_ptr<const case_insensitive_map_t<string>> properties) {
	if (blob_type != THETA_BLOB_TYPE || !properties) {
		return Value(LogicalType::BIGINT);
	}
	auto entry = properties->find("ndv");
	if (entry == properties->end() || entry->second.empty()) {
		return Value(LogicalType::BIGINT);
	}
	for (auto c : entry->second) {
		if (c < '0' || c > '9') {
			return Value(LogicalType::BIGINT);
		}
	}
	try {
		return Value::BIGINT(std::stoll(entry->second));
	} catch (...) {
		return Value(LogicalType::BIGINT);
	}
}

static bool BlobMetadataMatches(const rest_api_objects::BlobMetadata &expected,
                                const IcebergPuffinBlobMetadata &actual) {
	return expected.type == actual.type && expected.snapshot_id == actual.snapshot_id &&
	       expected.sequence_number == actual.sequence_number && expected.fields == actual.fields;
}

static idx_t FindMatchingBlob(const rest_api_objects::BlobMetadata &registered_blob,
                              const vector<IcebergPuffinBlobMetadata> &puffin_blobs, vector<bool> &blob_used) {
	for (idx_t i = 0; i < puffin_blobs.size(); i++) {
		if (!blob_used[i] && BlobMetadataMatches(registered_blob, puffin_blobs[i])) {
			blob_used[i] = true;
			return i;
		}
	}
	throw InvalidConfigurationException(
	    "Statistics file metadata could not be matched to a Puffin blob for type '%s', snapshot %lld, sequence %lld",
	    registered_blob.type, registered_blob.snapshot_id, registered_blob.sequence_number);
}

static unique_ptr<FunctionData> IcebergTableStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<IcebergTableStatsBindData>();

	auto &fs = FileSystem::GetFileSystem(context);
	auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(fs, *context.db);
	auto input_string = input.inputs[0].ToString();
	auto filename = IcebergUtils::GetStorageLocation(context, input_string);

	IcebergOptions options;
	ParseTableFunctionOptions(options, input.named_parameters);

	auto iceberg_meta_path = IcebergTableMetadata::GetMetaDataPath(context, filename, fs, options);
	auto table_metadata =
	    IcebergTableMetadata::Parse(iceberg_meta_path, *caching_fs, options.metadata_compression_codec);
	auto metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);

	optional<int64_t> requested_snapshot_id;
	bool no_matching_snapshot = false;
	if (!options.snapshot_lookup.IsLatest()) {
		auto snapshot = metadata.GetSnapshot(options.snapshot_lookup);
		if (!snapshot.snapshot) {
			no_matching_snapshot = true;
		} else {
			requested_snapshot_id = snapshot.snapshot->snapshot_id;
		}
	}

	if (!no_matching_snapshot) {
		for (auto &statistics_file : metadata.statistics) {
			if (requested_snapshot_id && statistics_file.snapshot_id != *requested_snapshot_id) {
				continue;
			}

			auto statistics_path = ResolveStatisticsPath(metadata, fs, options, statistics_file.statistics_path);
			auto puffin_footer = IcebergPuffinReader::ReadFooter(
			    fs, statistics_path, statistics_file.file_size_in_bytes, statistics_file.file_footer_size_in_bytes);
			auto &puffin_blobs = puffin_footer.file_metadata.blobs;
			vector<bool> blob_used(puffin_blobs.size(), false);

			for (auto &registered_blob : statistics_file.blob_metadata) {
				auto matched_blob_idx = FindMatchingBlob(registered_blob, puffin_blobs, blob_used);
				auto &puffin_blob = puffin_blobs[matched_blob_idx];
				optional_ptr<const case_insensitive_map_t<string>> effective_properties = nullptr;
				if (puffin_blob.properties) {
					effective_properties = &*puffin_blob.properties;
				} else if (registered_blob.properties) {
					effective_properties = &*registered_blob.properties;
				}

				IcebergTableStatsRow row;
				row.statistics_snapshot_id = Value::BIGINT(statistics_file.snapshot_id);
				row.statistics_path = Value(statistics_path);
				row.file_size_in_bytes = Value::BIGINT(statistics_file.file_size_in_bytes);
				row.file_footer_size_in_bytes = Value::BIGINT(statistics_file.file_footer_size_in_bytes);
				row.blob_index = Value::INTEGER(NumericCast<int32_t>(matched_blob_idx));
				row.blob_type = Value(puffin_blob.type);
				row.blob_snapshot_id = Value::BIGINT(puffin_blob.snapshot_id);
				row.blob_sequence_number = Value::BIGINT(puffin_blob.sequence_number);
				row.field_ids = IntListToValue(puffin_blob.fields);
				row.field_names = ResolveFieldNames(metadata, puffin_blob.fields, puffin_blob.snapshot_id);
				row.ndv = ParseNDV(puffin_blob.type, effective_properties);
				row.properties = PropertiesToValue(effective_properties);
				row.puffin_offset = Value::BIGINT(puffin_blob.offset);
				row.puffin_length = Value::BIGINT(puffin_blob.length);
				row.puffin_compression_codec =
				    puffin_blob.compression_codec ? Value(*puffin_blob.compression_codec) : Value(LogicalType::VARCHAR);
				bind_data->rows.emplace_back(std::move(row));
			}
		}
	}

	names = {"statistics_snapshot_id",
	         "statistics_path",
	         "file_size_in_bytes",
	         "file_footer_size_in_bytes",
	         "blob_index",
	         "blob_type",
	         "blob_snapshot_id",
	         "blob_sequence_number",
	         "field_ids",
	         "field_names",
	         "ndv",
	         "properties",
	         "puffin_offset",
	         "puffin_length",
	         "puffin_compression_codec"};
	return_types = {LogicalType::BIGINT,
	                LogicalType::VARCHAR,
	                LogicalType::BIGINT,
	                LogicalType::BIGINT,
	                LogicalType::INTEGER,
	                LogicalType::VARCHAR,
	                LogicalType::BIGINT,
	                LogicalType::BIGINT,
	                LogicalType::LIST(LogicalType::INTEGER),
	                LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::BIGINT,
	                LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR),
	                LogicalType::BIGINT,
	                LogicalType::BIGINT,
	                LogicalType::VARCHAR};
	return std::move(bind_data);
}

static void IcebergTableStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	(void)context;
	auto &global_state = data.global_state->Cast<IcebergTableStatsGlobalState>();
	auto &rows = global_state.bind_data.rows;

	idx_t out = 0;
	for (; global_state.offset < rows.size() && out < STANDARD_VECTOR_SIZE; global_state.offset++, out++) {
		auto &row = rows[global_state.offset];
		output.data[0].SetValue(out, row.statistics_snapshot_id);
		output.data[1].SetValue(out, row.statistics_path);
		output.data[2].SetValue(out, row.file_size_in_bytes);
		output.data[3].SetValue(out, row.file_footer_size_in_bytes);
		output.data[4].SetValue(out, row.blob_index);
		output.data[5].SetValue(out, row.blob_type);
		output.data[6].SetValue(out, row.blob_snapshot_id);
		output.data[7].SetValue(out, row.blob_sequence_number);
		output.data[8].SetValue(out, row.field_ids);
		output.data[9].SetValue(out, row.field_names);
		output.data[10].SetValue(out, row.ndv);
		output.data[11].SetValue(out, row.properties);
		output.data[12].SetValue(out, row.puffin_offset);
		output.data[13].SetValue(out, row.puffin_length);
		output.data[14].SetValue(out, row.puffin_compression_codec);
	}
	output.SetCardinality(out);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergTableStatsFunction() {
	TableFunctionSet function_set("iceberg_table_stats");
	TableFunction fun({LogicalType::VARCHAR}, IcebergTableStatsFunction, IcebergTableStatsBind,
	                  IcebergTableStatsGlobalState::Init);

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
