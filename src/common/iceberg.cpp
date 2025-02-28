#include "duckdb.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_manifest.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_types.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

template <class OP>
vector<typename OP::entry_type> ScanAvroMetadata(ClientContext &context, const string &path) {
	vector<typename OP::entry_type> ret;

	// TODO: make streaming
	auto &instance = DatabaseInstance::GetDatabase(context);
	auto &avro_scan_entry = ExtensionUtil::GetTableFunction(instance, "read_avro");
	auto &avro_scan = avro_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = StringUtil::Format("%sV%d", OP::NAME, OP::FORMAT_VERSION);
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	vector<LogicalType> return_types;
	vector<string> return_names;

	auto bind_data = avro_scan.bind(context, bind_input, return_types, return_names);

	DataChunk result;
	result.Initialize(context, return_types, STANDARD_VECTOR_SIZE);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	case_insensitive_map_t<ColumnIndex> name_to_vec;
	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
		OP::PopulateNameMapping(i, return_types[i], return_names[i], name_to_vec);
	}

	if (!OP::VerifySchema(name_to_vec)) {
		throw InvalidInputException("%s schema invalid for Iceberg version %d", OP::NAME, OP::FORMAT_VERSION);
	}

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = avro_scan.init_global(context, input);

	do {
		TableFunctionInput function_input(bind_data.get(), nullptr, global_state.get());
		result.Reset();
		avro_scan.function(context, function_input, result);

		idx_t count = result.size();
		for (auto &vec : result.data) {
			vec.Flatten(count);
		}

		OP::ProduceEntries(result, name_to_vec, ret);
	} while (result.size() != 0);
	return ret;
}

template <class OP>
static void ReadManifestEntries(ClientContext &context, const vector<IcebergManifest> &manifests, bool allow_moved_paths, FileSystem &fs, const string &iceberg_path, vector<IcebergTableEntry> &result) {
	for (auto &manifest : manifests) {
		auto manifest_entry_full_path = allow_moved_paths
		                                    ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
		                                    : manifest.manifest_path;
		auto manifest_paths = ScanAvroMetadata<OP>(context, manifest_entry_full_path);
		result.push_back({std::move(manifest), std::move(manifest_paths)});
	}
}

IcebergTable IcebergTable::Load(const string &iceberg_path, IcebergSnapshot &snapshot, ClientContext &context,
                                bool allow_moved_paths, string metadata_compression_codec) {
	IcebergTable ret;
	ret.path = iceberg_path;
	ret.snapshot = snapshot;

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;
	vector<IcebergManifest> manifests;
	if (snapshot.iceberg_format_version == 1) {
		manifests = ScanAvroMetadata<IcebergManifestV1>(context, manifest_list_full_path);
		ReadManifestEntries<IcebergManifestEntryV1>(context, manifests, allow_moved_paths, fs, iceberg_path, ret.entries);
	} else if (snapshot.iceberg_format_version == 2) {
		manifests = ScanAvroMetadata<IcebergManifestV2>(context, manifest_list_full_path);
		ReadManifestEntries<IcebergManifestEntryV2>(context, manifests, allow_moved_paths, fs, iceberg_path, ret.entries);
	} else {
		throw InvalidInputException("iceberg_format_version %d not handled", snapshot.iceberg_format_version);
	}

	return ret;
}

unique_ptr<SnapshotParseInfo> IcebergSnapshot::GetParseInfo(yyjson_doc &metadata_json) {
	SnapshotParseInfo info {};
	auto root = yyjson_doc_get_root(&metadata_json);
	info.iceberg_version = IcebergUtils::TryGetNumFromObject(root, "format-version");
	info.snapshots = yyjson_obj_get(root, "snapshots");

	// Multiple schemas can be present in the json metadata 'schemas' list
	if (yyjson_obj_getn(root, "current-schema-id", string("current-schema-id").size())) {
		size_t idx, max;
		yyjson_val *schema;
		info.schema_id = IcebergUtils::TryGetNumFromObject(root, "current-schema-id");
		auto schemas = yyjson_obj_get(root, "schemas");
		yyjson_arr_foreach(schemas, idx, max, schema) {
			info.schemas.push_back(schema);
		}
	} else {
		auto schema = yyjson_obj_get(root, "schema");
		if (!schema) {
			throw IOException("Neither a valid schema or schemas field was found");
		}
		auto found_schema_id = IcebergUtils::TryGetNumFromObject(schema, "schema-id");
		info.schemas.push_back(schema);
		info.schema_id = found_schema_id;
	}

	return make_uniq<SnapshotParseInfo>(std::move(info));
}

unique_ptr<SnapshotParseInfo> IcebergSnapshot::GetParseInfo(const string &path, FileSystem &fs, string metadata_compression_codec) {
	auto metadata_json = ReadMetaData(path, fs, metadata_compression_codec);
	auto* doc = yyjson_read(metadata_json.c_str(), metadata_json.size(), 0);
	if (doc == nullptr) {
		throw InvalidInputException("Fails to parse iceberg metadata from %s", path);
	}
	auto parse_info = GetParseInfo(*doc);

	// Transfer string and yyjson doc ownership
	parse_info->doc = doc;
	parse_info->document = std::move(metadata_json);

	return parse_info;
}

IcebergSnapshot IcebergSnapshot::GetLatestSnapshot(const string &path, FileSystem &fs,
	string metadata_compression_codec, bool skip_schema_inference) {
	auto info = GetParseInfo(path, fs, metadata_compression_codec);
	auto latest_snapshot = FindLatestSnapshotInternal(info->snapshots);

	if (!latest_snapshot) {
		throw IOException("No snapshots found");
	}

	return ParseSnapShot(latest_snapshot, info->iceberg_version, info->schema_id, info->schemas, metadata_compression_codec, skip_schema_inference);
}

IcebergSnapshot IcebergSnapshot::GetSnapshotById(const string &path, FileSystem &fs, idx_t snapshot_id,
	string metadata_compression_codec, bool skip_schema_inference) {
	auto info = GetParseInfo(path, fs, metadata_compression_codec);
	auto snapshot = FindSnapshotByIdInternal(info->snapshots, snapshot_id);

	if (!snapshot) {
		throw IOException("Could not find snapshot with id " + to_string(snapshot_id));
	}

	return ParseSnapShot(snapshot, info->iceberg_version, info->schema_id, info->schemas,
		metadata_compression_codec, skip_schema_inference);
}

IcebergSnapshot IcebergSnapshot::GetSnapshotByTimestamp(const string &path, FileSystem &fs, timestamp_t timestamp, string metadata_compression_codec,
	bool skip_schema_inference) {
	auto info = GetParseInfo(path, fs, metadata_compression_codec);
	auto snapshot = FindSnapshotByIdTimestampInternal(info->snapshots, timestamp);

	if (!snapshot) {
		throw IOException("Could not find latest snapshots for timestamp " + Timestamp::ToString(timestamp));
	}

	return ParseSnapShot(snapshot, info->iceberg_version, info->schema_id, info->schemas, metadata_compression_codec, skip_schema_inference);
}

// Function to generate a metadata file url from version and format string
// default format is "v%s%s.metadata.json" -> v00###-xxxxxxxxx-.gz.metadata.json"
string GenerateMetaDataUrl(FileSystem &fs, const string &meta_path, string &table_version, string &metadata_compression_codec, string &version_format = DEFAULT_TABLE_VERSION_FORMAT) {
	// TODO: Need to URL Encode table_version
	string compression_suffix = "";
	string url;
	if (metadata_compression_codec == "gzip") {
		compression_suffix = ".gz";
	}
	for(auto try_format : StringUtil::Split(version_format, ',')) {
		url = fs.JoinPath(meta_path, StringUtil::Format(try_format, table_version, compression_suffix));
		if(fs.FileExists(url)) {
			return url;
		}
	}

	throw IOException(
		"Iceberg metadata file not found for table version '%s' using '%s' compression and format(s): '%s'", table_version, metadata_compression_codec, version_format);
}


string IcebergSnapshot::GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs, string metadata_compression_codec, string table_version = DEFAULT_TABLE_VERSION, string version_format = DEFAULT_TABLE_VERSION_FORMAT) {
	string version_hint;
	string meta_path = fs.JoinPath(path, "metadata");
	if (StringUtil::EndsWith(path, ".json")) {
		// We've been given a real metadata path. Nothing else to do.
		return path;
	}
	if(StringUtil::EndsWith(table_version, ".text")||StringUtil::EndsWith(table_version, ".txt")) {
		// We were given a hint filename
		version_hint = GetTableVersionFromHint(meta_path, fs, table_version);
		return GenerateMetaDataUrl(fs, meta_path, version_hint, metadata_compression_codec, version_format);
	}
	if (table_version != UNKNOWN_TABLE_VERSION) {
		// We were given an explicit version number
		version_hint = table_version;
		return GenerateMetaDataUrl(fs, meta_path, version_hint, metadata_compression_codec, version_format);
	}
	if (fs.FileExists(fs.JoinPath(meta_path, DEFAULT_VERSION_HINT_FILE))) {
		// We're guessing, but a version-hint.text exists so we'll use that
		version_hint = GetTableVersionFromHint(meta_path, fs, DEFAULT_VERSION_HINT_FILE);
		return GenerateMetaDataUrl(fs, meta_path, version_hint, metadata_compression_codec, version_format);
	}
	if (!UnsafeVersionGuessingEnabled(context)) {
		// Make sure we're allowed to guess versions
		throw IOException("Failed to read iceberg table. No version was provided and no version-hint could be found, globbing the filesystem to locate the latest version is disabled by default as this is considered unsafe and could result in reading uncommitted data. To enable this use 'SET %s = true;'", VERSION_GUESSING_CONFIG_VARIABLE);
	}

	// We are allowed to guess to guess from file paths
	return GuessTableVersion(meta_path, fs, table_version, metadata_compression_codec, version_format);
}


string IcebergSnapshot::ReadMetaData(const string &path, FileSystem &fs, string metadata_compression_codec) {
	if (metadata_compression_codec == "gzip") {
		return IcebergUtils::GzFileToString(path, fs);
	}
	return IcebergUtils::FileToString(path, fs);
}


IcebergSnapshot IcebergSnapshot::ParseSnapShot(yyjson_val *snapshot, idx_t iceberg_format_version, idx_t schema_id,
                                               vector<yyjson_val *> &schemas, string metadata_compression_codec,
											   bool skip_schema_inference) {
	IcebergSnapshot ret;
	auto snapshot_tag = yyjson_get_type(snapshot);
	if (snapshot_tag != YYJSON_TYPE_OBJ) {
		throw IOException("Invalid snapshot field found parsing iceberg metadata.json");
	}
	ret.metadata_compression_codec = metadata_compression_codec;
	if (iceberg_format_version == 1) {
		ret.sequence_number = 0;
	} else if (iceberg_format_version == 2) {
		ret.sequence_number = IcebergUtils::TryGetNumFromObject(snapshot, "sequence-number");
	}

	ret.snapshot_id = IcebergUtils::TryGetNumFromObject(snapshot, "snapshot-id");
	ret.timestamp_ms = Timestamp::FromEpochMs(IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms"));
	ret.manifest_list = IcebergUtils::TryGetStrFromObject(snapshot, "manifest-list");
	ret.iceberg_format_version = iceberg_format_version;
	ret.schema_id = schema_id;
	if (!skip_schema_inference) {
		ret.schema = ParseSchema(schemas, ret.schema_id);
	}
	return ret;
}

string IcebergSnapshot::GetTableVersionFromHint(const string &meta_path, FileSystem &fs, string version_file = DEFAULT_VERSION_HINT_FILE) {
	auto version_file_path = fs.JoinPath(meta_path, version_file);
	auto version_file_content = IcebergUtils::FileToString(version_file_path, fs);

	try {
		return version_file_content;
	} catch (std::invalid_argument &e) {
		throw IOException("Iceberg version hint file contains invalid value");
	} catch (std::out_of_range &e) {
		throw IOException("Iceberg version hint file contains invalid value");
	}
}

bool IcebergSnapshot::UnsafeVersionGuessingEnabled(ClientContext &context) {
	Value result;
	(void)context.TryGetCurrentSetting(VERSION_GUESSING_CONFIG_VARIABLE, result);
	return !result.IsNull() && result.GetValue<bool>();
}


string IcebergSnapshot::GuessTableVersion(const string &meta_path, FileSystem &fs, string &table_version, string &metadata_compression_codec, string &version_format = DEFAULT_TABLE_VERSION_FORMAT) {
	string selected_metadata;
	string version_pattern = "*"; // TODO: Different "table_version" strings could customize this
	string compression_suffix = "";
	

	if (metadata_compression_codec == "gzip") {
		compression_suffix = ".gz";
	}
	
	for(auto try_format : StringUtil::Split(version_format, ',')) {
		auto glob_pattern = StringUtil::Format(try_format, version_pattern, compression_suffix);
		
		auto found_versions = fs.Glob(fs.JoinPath(meta_path, glob_pattern));
		if(found_versions.size() > 0) {
			selected_metadata = PickTableVersion(found_versions, version_pattern, glob_pattern);
			if(!selected_metadata.empty()) {  // Found one
				return selected_metadata;
			}
		}
	}
	
	throw IOException(
	        "Could not guess Iceberg table version using '%s' compression and format(s): '%s'",
	        metadata_compression_codec, version_format);
}

string IcebergSnapshot::PickTableVersion(vector<string> &found_metadata, string &version_pattern, string &glob) {
	// TODO: Different "table_version" strings could customize this
	// For now: just sort the versions and take the largest
	if(!found_metadata.empty()) {
		std::sort(found_metadata.begin(), found_metadata.end());
		return found_metadata.back();
	} else {
		return string();
	}
}


yyjson_val *IcebergSnapshot::FindLatestSnapshotInternal(yyjson_val *snapshots) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_timestamp = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto timestamp = IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms");
		if (timestamp >= max_timestamp) {
			max_timestamp = timestamp;
			max_snapshot = snapshot;
		}
	}

	return max_snapshot;
}

yyjson_val *IcebergSnapshot::FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id) {
	size_t idx, max;
	yyjson_val *snapshot;

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {

		auto snapshot_id = IcebergUtils::TryGetNumFromObject(snapshot, "snapshot-id");

		if (snapshot_id == target_id) {
			return snapshot;
		}
	}

	return nullptr;
}

yyjson_val *IcebergSnapshot::IcebergSnapshot::FindSnapshotByIdTimestampInternal(yyjson_val *snapshots,
                                                                                timestamp_t timestamp) {
	size_t idx, max;
	yyjson_val *snapshot;

	uint64_t max_millis = NumericLimits<uint64_t>::Minimum();
	yyjson_val *max_snapshot = nullptr;

	auto timestamp_millis = Timestamp::GetEpochMs(timestamp);

	yyjson_arr_foreach(snapshots, idx, max, snapshot) {
		auto curr_millis = IcebergUtils::TryGetNumFromObject(snapshot, "timestamp-ms");

		if (curr_millis <= timestamp_millis && curr_millis >= max_millis) {
			max_snapshot = snapshot;
			max_millis = curr_millis;
		}
	}

	return max_snapshot;
}

} // namespace duckdb
