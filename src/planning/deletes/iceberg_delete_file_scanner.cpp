#include "planning/deletes/iceberg_delete_file_scanner.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "common/iceberg_utils.hpp"
#include "core/deletes/iceberg_deletion_vector.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "core/metadata/puffin/iceberg_puffin_metadata.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "function/iceberg_functions.hpp"
#include "iceberg_logging.hpp"
#include "iceberg_options.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"
#include "planning/metadata_io/deletes/iceberg_deletes_file_reader.hpp"

#include <variant>

namespace duckdb {

const IcebergManifestEntry &IcebergDeleteScanEntry::GetEntry() const {
	auto &manifest_entries = manifest.GetManifestEntries();
	if (entry_idx >= manifest_entries.size()) {
		throw InternalException("Delete manifest entry index %llu is out of bounds for manifest %llu", entry_idx,
		                        manifest_idx);
	}
	return manifest_entries[entry_idx];
}

BoundIcebergManifestEntry IcebergDeleteScanEntry::BindEntry() const {
	return BoundIcebergManifestListEntry(manifest_idx, manifest).BindEntry(GetEntry());
}

namespace {

using PuffinDeletionVectorVerificationResult = std::variant<std::monostate, string>;

static PuffinDeletionVectorVerificationResult VerifyPuffinDeletionVector(FileSystem &fs, FileHandle &handle,
                                                                         int64_t content_offset, int64_t content_size) {
	auto footer_result = IcebergPuffinReader::ReadFooter(fs, handle, handle.GetPath());
	if (auto error = std::get_if<string>(&footer_result)) {
		return *error;
	}
	auto &footer = std::get<IcebergPuffinFileFooter>(footer_result);

	bool contains_blob = false;
	for (auto &blob : footer.file_metadata.blobs) {
		if (blob.type != "deletion-vector-v1") {
			return "Deletion vector Puffin blob type mismatch: expected deletion-vector-v1";
		}
		if (blob.offset == content_offset && blob.length == content_size) {
			contains_blob = true;
		}
	}
	if (!contains_blob) {
		return StringUtil::Format("Deletion vector blob with offset (%d) and length (%d) not found in Puffin file",
		                          content_offset, content_size);
	}
	return std::monostate {};
}

static void ScanPuffinFile(const IcebergDeletePlanningContext &context, const IcebergDeleteScanEntry &scan_entry,
                           IcebergDeleteScanResult &scan_result) {
	auto bound_entry = scan_entry.BindEntry();
	auto &data_file = bound_entry.entry.data_file;
	if (context.metadata.iceberg_version < 3) {
		throw InvalidConfigurationException("DeletionVector not supported in Iceberg V%d",
		                                    context.metadata.iceberg_version);
	}
	if (!data_file.referenced_data_file) {
		throw InvalidConfigurationException("Puffin delete file is missing 'referenced_data_file'");
	}

	FileOpenFlags flags = FileFlags::FILE_FLAGS_READ;
	flags.SetCachingMode(CachingMode::CACHE_REMOTE_ONLY);
	auto file_handle = context.fs.OpenFile(data_file.file_path, flags);
	if (!data_file.content_offset) {
		throw InvalidConfigurationException("Puffin delete file is missing 'content_offset");
	}
	if (!data_file.content_size_in_bytes) {
		throw InvalidConfigurationException("Puffin delete file is missing 'content_size_in_bytes");
	}

	auto offset = *data_file.content_offset;
	auto length = *data_file.content_size_in_bytes;
	auto local_buffer = Allocator::DefaultAllocator().Allocate(length);
	context.fs.Read(*file_handle, local_buffer.get(), length, offset);

	Value skip_verification;
	if (!context.context.TryGetCurrentSetting(SKIP_PUFFIN_VERIFICATION_CONFIG_VARIABLE, skip_verification) ||
	    !skip_verification.GetValue<bool>()) {
		auto verification_result = VerifyPuffinDeletionVector(context.fs, *file_handle, offset, length);
		if (auto error = std::get_if<string>(&verification_result)) {
			throw InvalidConfigurationException("%s. Older versions of DuckDB wrote deletion vector files as bare "
			                                    "blobs. To read those files, run \"SET "
			                                    "%s = true\"",
			                                    *error, SKIP_PUFFIN_VERIFICATION_CONFIG_VARIABLE);
		}
	}

	auto &positional_delete_data = scan_result.positional_delete_data;
	auto it = positional_delete_data.find(*data_file.referenced_data_file);
	if (it != positional_delete_data.end() && it->second->type == IcebergDeleteType::DELETION_VECTOR) {
		throw InvalidConfigurationException(
		    "Table is corrupt, two or more deletion vectors exist for the same referenced_data_file");
	}
	positional_delete_data[*data_file.referenced_data_file] =
	    IcebergDeletionVectorData::FromBlob(bound_entry, local_buffer.get(), length);
}

static optional_ptr<IcebergPositionalDeleteData> TryGetOrCreatePositionDeletes(position_delete_map_t &deletes,
                                                                               const BoundIcebergManifestEntry &entry,
                                                                               const string &file_path) {
	auto it = deletes.find(file_path);
	if (it == deletes.end()) {
		it = deletes.emplace(file_path, make_shared_ptr<IcebergPositionalDeleteData>(entry)).first;
	} else if (it->second->type == IcebergDeleteType::POSITIONAL_DELETE) {
		it->second->entries.push_back(entry);
	}
	if (it->second->type != IcebergDeleteType::POSITIONAL_DELETE) {
		return nullptr;
	}
	return reinterpret_cast<IcebergPositionalDeleteData &>(*it->second);
}

static void ScanPositionalDeleteFile(const IcebergDeletePlanningContext &context,
                                     const IcebergDeleteScanEntry &scan_entry, DataChunk &result,
                                     IcebergDeleteScanResult &scan_result) {
	auto bound_entry = scan_entry.BindEntry();
	auto names = FlatVector::GetData<string_t>(result.data[0]);
	auto row_ids = FlatVector::GetData<int64_t>(result.data[1]);
	if (result.size() == 0) {
		return;
	}

	reference<const string_t> current_file_path = names[0];
	auto initial_key = current_file_path.get().GetString();
	auto &positional_delete_data = scan_result.positional_delete_data;
	auto deletes = TryGetOrCreatePositionDeletes(positional_delete_data, bound_entry, initial_key);
	DUCKDB_LOG(context.context, IcebergLogType,
	           "Iceberg Delete Scan, read 'positional_delete_file': '%s', referencing 'data_file': '%s'",
	           bound_entry.entry.data_file.file_path, initial_key);

	for (idx_t i = 0; i < result.size(); i++) {
		auto &name = names[i];
		if (name != current_file_path.get()) {
			current_file_path = name;
			auto key = current_file_path.get().GetString();
			DUCKDB_LOG(context.context, IcebergLogType,
			           "Iceberg Delete Scan, read 'positional_delete_file': '%s', referencing 'data_file': '%s'",
			           bound_entry.entry.data_file.file_path, key);
			deletes = TryGetOrCreatePositionDeletes(positional_delete_data, bound_entry, key);
		}
		if (deletes) {
			deletes->AddRow(row_ids[i]);
		}
	}
}

static void InitializeFromOtherChunk(DataChunk &target, DataChunk &other, const vector<column_t> &column_ids) {
	vector<LogicalType> types;
	for (auto id : column_ids) {
		types.push_back(other.data[id].GetType());
	}
	target.InitializeEmpty(types);
}

static void ColumnsReferencedByEqualityIds(DataChunk &source, DataChunk &result,
                                           const vector<MultiFileColumnDefinition> &global_columns,
                                           const vector<int32_t> &equality_ids) {
	D_ASSERT(source.ColumnCount() == global_columns.size());
	unordered_map<int32_t, column_t> id_to_column;
	for (column_t column_idx = 0; column_idx < global_columns.size(); column_idx++) {
		auto &column = global_columns[column_idx];
		D_ASSERT(!column.identifier.IsNull());
		id_to_column[column.identifier.GetValue<int32_t>()] = column_idx;
	}

	vector<column_t> column_ids;
	for (auto id : equality_ids) {
		auto entry = id_to_column.find(id);
		if (entry == id_to_column.end()) {
			throw InternalException("Equality-delete field id %d is missing from the global delete schema", id);
		}
		column_ids.push_back(entry->second);
	}
	InitializeFromOtherChunk(result, source, column_ids);
	result.ReferenceColumns(source, column_ids);
}

static void ScanEqualityDeleteFile(const IcebergDeletePlanningContext &context,
                                   const IcebergDeleteScanEntry &scan_entry, IcebergEqualityDeleteFile &delete_file,
                                   DataChunk &source, const vector<MultiFileColumnDefinition> &global_columns) {
	auto &manifest_entry = scan_entry.GetEntry();
	auto &data_file = manifest_entry.data_file;
	D_ASSERT(!data_file.equality_ids.empty());
	D_ASSERT(source.ColumnCount() == global_columns.size());
	if (source.size() == 0) {
		return;
	}

	DataChunk result;
	ColumnsReferencedByEqualityIds(source, result, global_columns, data_file.equality_ids);
	auto &equality_values = delete_file.equality_values;
	if (data_file.record_count < 0) {
		throw InvalidConfigurationException("Equality delete file '%s' has a negative record count",
		                                    data_file.file_path);
	}
	auto expected_row_count = NumericCast<idx_t>(data_file.record_count);
	if (equality_values.size() > expected_row_count || source.size() > expected_row_count - equality_values.size()) {
		throw InvalidConfigurationException(
		    "Equality delete file '%s' contains more rows than its record count of %llu", data_file.file_path,
		    expected_row_count);
	}
	if (equality_values.ColumnCount() == 0) {
		equality_values.Initialize(context.context, result.GetTypes(), expected_row_count);
	} else {
		if (equality_values.ColumnCount() != result.ColumnCount()) {
			throw InvalidConfigurationException("Equality delete file '%s' produced chunks with differing schemas",
			                                    data_file.file_path);
		}
		for (idx_t column_idx = 0; column_idx < result.ColumnCount(); column_idx++) {
			if (equality_values.data[column_idx].GetType() != result.data[column_idx].GetType()) {
				throw InvalidConfigurationException("Equality delete file '%s' produced chunks with differing schemas",
				                                    data_file.file_path);
			}
		}
	}
	equality_values.Append(result, VectorAppendMode::ERROR_ON_NO_SPACE);
}

static vector<MultiFileColumnDefinition>
BuildEqualityDeleteSchema(const IcebergTableMetadata &metadata,
                          const vector<reference<const IcebergDeleteScanEntry>> &scan_entries) {
	vector<MultiFileColumnDefinition> schema;
	unordered_set<int32_t> field_ids;
	for (auto &scan_entry_ref : scan_entries) {
		auto &data_file = scan_entry_ref.get().GetEntry().data_file;
		for (auto field_id : data_file.equality_ids) {
			if (!field_ids.insert(field_id).second) {
				continue;
			}
			auto column = metadata.FindColumnByFieldId(field_id);
			if (!column) {
				throw InvalidConfigurationException(
				    "Equality-delete file '%s' references field id %d, but no table schema contains that field",
				    data_file.file_path, field_id);
			}
			auto schema_column = column->GetMultiFileColumnDefinition();
			schema_column.name = Identifier(StringUtil::Format("r%d", field_id));
			schema_column.default_expression = make_uniq<ConstantExpression>(Value(schema_column.type));
			schema.push_back(std::move(schema_column));
		}
	}
	return schema;
}

static vector<MultiFileColumnDefinition> BuildPositionalDeleteSchema() {
	vector<MultiFileColumnDefinition> schema;
	MultiFileColumnDefinition file_path("file_path", LogicalType::VARCHAR);
	file_path.identifier = Value::INTEGER(MultiFileReader::DELETE_FILE_PATH_FIELD_ID);
	schema.push_back(std::move(file_path));
	MultiFileColumnDefinition pos("pos", LogicalType::BIGINT);
	pos.identifier = Value::INTEGER(MultiFileReader::DELETE_POS_FIELD_ID);
	schema.push_back(std::move(pos));
	return schema;
}

static void ScanParquetDeleteFiles(const IcebergDeletePlanningContext &context,
                                   const vector<reference<const IcebergDeleteScanEntry>> &scan_entries,
                                   IcebergManifestEntryContentType content, IcebergDeleteScanResult &scan_result) {
	if (scan_entries.empty()) {
		return;
	}
	vector<Value> delete_file_paths;
	vector<OpenFileInfo> delete_file_infos;
	delete_file_paths.reserve(scan_entries.size());
	delete_file_infos.reserve(scan_entries.size());
	for (auto &scan_entry_ref : scan_entries) {
		auto &data_file = scan_entry_ref.get().GetEntry().data_file;
		D_ASSERT(data_file.content == content);
		auto delete_file_path = data_file.file_path;
		if (context.options.allow_moved_paths) {
			delete_file_path = IcebergUtils::GetFullPath(context.table_path, delete_file_path, context.fs);
		}
		delete_file_paths.emplace_back(delete_file_path);
		delete_file_infos.emplace_back(delete_file_path);
		auto &file_info = delete_file_infos.back();
		file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		file_info.extended_info->options["file_size"] = Value::UBIGINT(data_file.file_size_in_bytes);
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		file_info.extended_info->options["etag"] = Value("");
		file_info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	}

	auto iceberg_deletes_scan = IcebergFunctions::GetIcebergDeletesScanFunction(context.context);
	auto delete_scan_function =
	    iceberg_deletes_scan.GetFunctionByArguments(context.context, {LogicalType::LIST(LogicalType::VARCHAR)});
	vector<MultiFileColumnDefinition> delete_schema = content == IcebergManifestEntryContentType::POSITION_DELETES
	                                                      ? BuildPositionalDeleteSchema()
	                                                      : BuildEqualityDeleteSchema(context.metadata, scan_entries);

	vector<Value> children;
	children.push_back(Value::LIST(LogicalType::VARCHAR, std::move(delete_file_paths)));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef empty;
	auto delete_info = make_shared_ptr<IcebergDeleteScanInfo>(std::move(delete_file_infos), std::move(delete_schema));
	delete_scan_function.function_info = delete_info;

	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  delete_scan_function, empty);
	vector<LogicalType> return_types;
	vector<Identifier> return_names;
	auto bind_data = delete_scan_function.bind(context.context, bind_input, return_types, return_names);
	auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();

	DataChunk result;
	result.Initialize(context.context, return_types, STANDARD_VECTOR_SIZE);
	ThreadContext thread_context(context.context);
	ExecutionContext execution_context(context.context, thread_context, nullptr);
	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}
	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = delete_scan_function.init_global(context.context, input);
	auto local_state = delete_scan_function.init_local(execution_context, input, global_state.get());
	auto &multi_file_local_state = local_state->Cast<MultiFileLocalState>();

	vector<reference<IcebergEqualityDeleteFile>> equality_delete_files;
	if (content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
		for (auto &scan_entry_ref : scan_entries) {
			auto &scan_entry = scan_entry_ref.get();
			auto equality_delete =
			    make_shared_ptr<IcebergEqualityDeleteFile>(scan_entry.GetEntry().data_file.equality_ids);
			equality_delete_files.emplace_back(*equality_delete);
			scan_result.equality_delete_data.push_back({scan_entry.load, std::move(equality_delete)});
		}
	}

	while (true) {
		TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
		result.Reset();
		delete_scan_function.function(context.context, function_input, result);
		if (result.size() == 0) {
			break;
		}
		result.Flatten();
		auto file_idx = multi_file_local_state.job.reader->file_list_idx.GetIndex();
		if (file_idx >= scan_entries.size()) {
			throw InternalException("Delete batch reader index %llu is out of bounds for %llu files", file_idx,
			                        scan_entries.size());
		}
		auto &scan_entry = scan_entries[file_idx].get();
		if (content == IcebergManifestEntryContentType::POSITION_DELETES) {
			ScanPositionalDeleteFile(context, scan_entry, result, scan_result);
		} else {
			ScanEqualityDeleteFile(context, scan_entry, equality_delete_files[file_idx].get(), result,
			                       multi_file_bind_data.reader_bind.schema);
		}
	}
}

} // namespace

IcebergDeleteScanResult IcebergDeleteFileScanner::ScanFiles(const IcebergDeletePlanningContext &context,
                                                            const vector<IcebergDeleteScanEntry> &entries) {
	IcebergDeleteScanResult result;
	vector<reference<const IcebergDeleteScanEntry>> positional_delete_entries;
	vector<reference<const IcebergDeleteScanEntry>> equality_delete_entries;
	for (auto &scan_entry : entries) {
		auto &data_file = scan_entry.GetEntry().data_file;
		if (StringUtil::CIEquals(data_file.file_format, "parquet")) {
			switch (data_file.content) {
			case IcebergManifestEntryContentType::POSITION_DELETES:
				positional_delete_entries.emplace_back(scan_entry);
				break;
			case IcebergManifestEntryContentType::EQUALITY_DELETES:
				equality_delete_entries.emplace_back(scan_entry);
				break;
			default:
				throw InvalidConfigurationException("Delete manifest references Parquet file '%s' with content type %d",
				                                    data_file.file_path, static_cast<uint8_t>(data_file.content));
			}
		} else if (StringUtil::CIEquals(data_file.file_format, "puffin")) {
			ScanPuffinFile(context, scan_entry, result);
		} else {
			throw NotImplementedException(
			    "File format '%s' not supported for deletes, only supports 'parquet' and 'puffin' currently",
			    data_file.file_format);
		}
	}
	ScanParquetDeleteFiles(context, positional_delete_entries, IcebergManifestEntryContentType::POSITION_DELETES,
	                       result);
	ScanParquetDeleteFiles(context, equality_delete_entries, IcebergManifestEntryContentType::EQUALITY_DELETES, result);
	return result;
}

} // namespace duckdb
