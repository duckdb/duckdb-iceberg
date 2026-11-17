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
#include "planning/metadata_io/deletes/iceberg_deletes_file_reader.hpp"

#include "duckdb/parallel/task_executor.hpp"

#include <variant>

namespace duckdb {

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

static void ScanPuffinFile(const IcebergDeleteExecutionContext &context, const IcebergDeleteScanEntry &scan_entry) {
	auto &data_file = scan_entry.file;
	if (context.metadata.iceberg_version < 3) {
		throw InvalidConfigurationException("DeletionVector not supported in Iceberg V%d",
		                                    context.metadata.iceberg_version);
	}
	if (!data_file.referenced_data_file) {
		throw InvalidConfigurationException("Puffin delete file is missing 'referenced_data_file'");
	}

	auto puffin_path = data_file.file_path;
	if (context.options.allow_moved_paths) {
		puffin_path = IcebergUtils::GetFullPath(context.table_path, puffin_path, context.fs);
	}

	FileOpenFlags flags = FileFlags::FILE_FLAGS_READ;
	flags.SetCachingMode(CachingMode::CACHE_REMOTE_ONLY);
	auto puffin_file = OpenFileInfo(puffin_path);
	context.file_secret.Apply(puffin_file);
	auto file_handle = context.fs.OpenFile(puffin_file, flags);
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

	auto &positional_delete_data = scan_entry.load->positional_deletes;
	auto it = positional_delete_data.find(*data_file.referenced_data_file);
	if (it != positional_delete_data.end() && it->second->type == IcebergDeleteType::DELETION_VECTOR) {
		throw InvalidConfigurationException(
		    "Table is corrupt, two or more deletion vectors exist for the same referenced_data_file");
	}
	positional_delete_data[*data_file.referenced_data_file] = IcebergDeletionVectorData::FromBlob(
	    {data_file.file_path, data_file.content_offset}, local_buffer.get(), length);
}

static optional_ptr<IcebergPositionalDeleteData> TryGetOrCreatePositionDeletes(position_delete_map_t &deletes,
                                                                               const IcebergDeleteFile &delete_file,
                                                                               const string &file_path) {
	auto it = deletes.find(file_path);
	if (it == deletes.end()) {
		it = deletes.emplace(file_path, make_shared_ptr<IcebergPositionalDeleteData>()).first;
	}
	if (it->second->type != IcebergDeleteType::POSITIONAL_DELETE) {
		return nullptr;
	}
	// A deletion vector written for this data file supersedes a file-scoped delete file. A partition-scoped one
	// may still apply to other data files and stays live.
	if (delete_file.single_referenced_data_file == file_path) {
		it->second->source_files.push_back(delete_file.file_path);
	}
	return reinterpret_cast<IcebergPositionalDeleteData &>(*it->second);
}

static void ScanPositionalDeleteFile(const IcebergDeleteExecutionContext &context,
                                     const IcebergDeleteScanEntry &scan_entry, DataChunk &result) {
	auto &data_file = scan_entry.file;
	auto names = FlatVector::GetData<string_t>(result.data[0]);
	auto row_ids = FlatVector::GetData<int64_t>(result.data[1]);
	if (result.size() == 0) {
		return;
	}

	reference<const string_t> current_file_path = names[0];
	auto initial_key = current_file_path.get().GetString();
	auto &positional_delete_data = scan_entry.load->positional_deletes;
	auto deletes = TryGetOrCreatePositionDeletes(positional_delete_data, data_file, initial_key);
	DUCKDB_LOG(context.context, IcebergLogType,
	           "Iceberg Delete Scan, read 'positional_delete_file': '%s', referencing 'data_file': '%s'",
	           data_file.file_path, initial_key);

	for (idx_t i = 0; i < result.size(); i++) {
		auto &name = names[i];
		if (name != current_file_path.get()) {
			current_file_path = name;
			auto key = current_file_path.get().GetString();
			DUCKDB_LOG(context.context, IcebergLogType,
			           "Iceberg Delete Scan, read 'positional_delete_file': '%s', referencing 'data_file': '%s'",
			           data_file.file_path, key);
			deletes = TryGetOrCreatePositionDeletes(positional_delete_data, data_file, key);
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
			throw InvalidConfigurationException("Equality-delete field id %d is missing from the global delete schema",
			                                    id);
		}
		column_ids.push_back(entry->second);
	}
	InitializeFromOtherChunk(result, source, column_ids);
	result.ReferenceColumns(source, column_ids);
}

static void ScanEqualityDeleteFile(const IcebergDeleteExecutionContext &context,
                                   const IcebergDeleteScanEntry &scan_entry, IcebergEqualityDeleteFile &delete_file,
                                   DataChunk &source, const vector<MultiFileColumnDefinition> &global_columns) {
	auto &data_file = scan_entry.file;
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
BuildEqualityDeleteSchema(const IcebergTableMetadataSchemas &schemas,
                          const vector<reference<const IcebergDeleteScanEntry>> &scan_entries) {
	vector<MultiFileColumnDefinition> schema;
	unordered_set<int32_t> field_ids;
	for (auto &scan_entry_ref : scan_entries) {
		auto &data_file = scan_entry_ref.get().file;
		for (auto field_id : data_file.equality_ids) {
			if (!field_ids.insert(field_id).second) {
				continue;
			}
			auto column = schemas.FindColumnByFieldId(field_id);
			if (!column) {
				throw InvalidConfigurationException(
				    "Equality-delete file '%s' references field id %d, but no table schema contains that field",
				    data_file.file_path, field_id);
			}
			auto schema_column = column->GetMultiFileColumnDefinition();
			schema_column.name = Identifier(StringUtil::Format("r%d", field_id));
			schema_column.default_expression = ConstantExpression::FromValue(Value(schema_column.type));
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

static void ScanParquetDeleteFiles(const IcebergDeleteExecutionContext &context,
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
		auto &data_file = scan_entry_ref.get().file;
		D_ASSERT(data_file.content == content);
		auto delete_file_path = data_file.file_path;
		if (context.options.allow_moved_paths) {
			delete_file_path = IcebergUtils::GetFullPath(context.table_path, delete_file_path, context.fs);
		}
		delete_file_paths.emplace_back(delete_file_path);
		delete_file_infos.emplace_back(delete_file_path);
		auto &file_info = delete_file_infos.back();
		file_info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		context.file_secret.Apply(file_info);
		file_info.extended_info->options["file_size"] = Value::UBIGINT(data_file.file_size_in_bytes);
		file_info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		file_info.extended_info->options["etag"] = Value("");
		file_info.extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	}

	auto iceberg_deletes_scan = IcebergFunctions::GetIcebergDeletesScanFunction(context.context);
	// copied out of the local set: the bind mutates function_info and needs a mutable function
	auto delete_scan_function =
	    *iceberg_deletes_scan.GetFunctionByArguments(context.context, {LogicalType::LIST(LogicalType::VARCHAR)});
	vector<MultiFileColumnDefinition> delete_schema =
	    content == IcebergManifestEntryContentType::POSITION_DELETES
	        ? BuildPositionalDeleteSchema()
	        : BuildEqualityDeleteSchema(context.metadata.GetSchemas(), scan_entries);

	vector<Value> children;
	children.push_back(Value::LIST(LogicalType::VARCHAR, std::move(delete_file_paths)));
	named_argument_map_t named_params;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef empty;
	auto delete_info = make_shared_ptr<IcebergDeleteScanInfo>(std::move(delete_file_infos), std::move(delete_schema));
	delete_scan_function.function_info = delete_info;

	// the bind sees the function as a bound call would; nothing is read back off it afterwards
	BoundTableFunction bound_delete_scan_function(delete_scan_function);
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  bound_delete_scan_function, empty);
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
			auto &data_file = scan_entry.file;
			auto equality_delete =
			    make_shared_ptr<IcebergEqualityDeleteFile>(data_file.file_path, data_file.equality_ids);
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
		auto file_idx = multi_file_local_state.job->reader->file_list_idx.GetIndex();
		if (file_idx >= scan_entries.size()) {
			throw InternalException("Delete batch reader index %llu is out of bounds for %llu files", file_idx,
			                        scan_entries.size());
		}
		auto &scan_entry = scan_entries[file_idx].get();
		if (content == IcebergManifestEntryContentType::POSITION_DELETES) {
			ScanPositionalDeleteFile(context, scan_entry, result);
		} else {
			ScanEqualityDeleteFile(context, scan_entry, equality_delete_files[file_idx].get(), result,
			                       multi_file_bind_data.reader_bind.schema);
		}
	}
}

} // namespace

IcebergDeleteScanResult IcebergDeleteFileScanner::ScanFiles(const IcebergDeleteExecutionContext &context,
                                                            const vector<IcebergDeleteScanEntry> &entries) {
	IcebergDeleteScanResult result;
	vector<reference<const IcebergDeleteScanEntry>> positional_delete_entries;
	vector<reference<const IcebergDeleteScanEntry>> equality_delete_entries;
	for (auto &scan_entry : entries) {
		auto &data_file = scan_entry.file;
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
			ScanPuffinFile(context, scan_entry);
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

namespace {

//! Publishes a load, unless it is published already: the workers that wait for the delete file continue.
static void CompleteDeleteFileLoad(IcebergDeleteFileLoadState &load, const ErrorData &error) {
	{
		lock_guard<mutex> guard(load.lock);
		if (load.complete) {
			return;
		}
		load.error = error;
		load.complete = true;
		load.read_tasks.reset();
	}
	load.cv.notify_all();
}

//! Reads delete files and publishes them. Throws, and publishes nothing, when the read fails.
static void ReadDeleteFiles(const IcebergDeleteExecutionContext &context,
                            const vector<IcebergDeleteScanEntry> &entries) {
	context.context.InterruptCheck();
	auto scanned = IcebergDeleteFileScanner::ScanFiles(context, entries);
	for (auto &entry : scanned.equality_delete_data) {
		if (entry.delete_file->equality_values.size()) {
			entry.load->equality_delete = std::move(entry.delete_file);
		}
	}
	for (auto &entry : entries) {
		CompleteDeleteFileLoad(*entry.load, ErrorData());
	}
}

//! Reads some of the delete files a worker claimed. It uses the execution context of that worker, which waits
//! for its read tasks before it returns. A read that fails fails the claim: the executor keeps the first error
//! and cancels the read tasks that have not started, and the claiming worker publishes that error.
class IcebergDeleteFileReadTask : public BaseExecutorTask {
public:
	IcebergDeleteFileReadTask(TaskExecutor &executor, const IcebergDeleteExecutionContext &context,
	                          vector<IcebergDeleteScanEntry> entries_p)
	    : BaseExecutorTask(executor), context(context), entries(std::move(entries_p)) {
	}

	void ExecuteTask() override {
		ReadDeleteFiles(context, entries);
	}

	string TaskType() const override {
		return "IcebergDeleteFileReadTask";
	}

private:
	const IcebergDeleteExecutionContext &context;
	const vector<IcebergDeleteScanEntry> entries;
};

//! Fails the delete files a worker claimed that nothing published. The worker calls this before it leaves with
//! an error, whatever the error is: a read task can fail without having run, and tasks can fail to be queued.
static void FailUnreadDeleteFileLoads(optional_ptr<TaskExecutor> read_tasks,
                                      const vector<shared_ptr<IcebergDeleteFileLoadState>> &claimed_loads,
                                      const ErrorData &error) {
	if (read_tasks) {
		// A read task that is still queued uses the context of the claiming worker.
		read_tasks->CancelAndDrain();
	}
	for (auto &load : claimed_loads) {
		CompleteDeleteFileLoad(*load, error);
	}
}

} // namespace

constexpr idx_t IcebergDeleteExecutionState::DELETE_FILES_PER_READ_TASK;

shared_ptr<IcebergDeleteData>
IcebergDeleteExecutionState::GetExistingPositionalDeleteData(const string &file_path) const {
	lock_guard<mutex> guard(lock);
	auto entry = positional_delete_data.find(file_path);
	return entry == positional_delete_data.end() ? nullptr : entry->second;
}

IcebergDeletePlan IcebergDeleteExecutionState::ProcessDeletes(const IcebergDeleteExecutionContext &context,
                                                              const string &data_file_path,
                                                              const vector<IcebergDeleteFile> &descriptors) {
	IcebergDeletePlan result;
	vector<IcebergDeleteScanEntry> scan_entries;
	vector<shared_ptr<IcebergDeleteFileLoadState>> required_loads;
	vector<shared_ptr<IcebergDeleteFileLoadState>> claimed_loads;
	shared_ptr<TaskExecutor> read_tasks;
	try {
		{
			lock_guard<mutex> guard(lock);
			unordered_set<IcebergDeleteFileLoadState *> seen;
			for (auto &descriptor : descriptors) {
				shared_ptr<IcebergDeleteFileLoadState> load;
				auto &bucket = descriptor_loads[{descriptor.file_path, descriptor.content_offset}];
				for (auto &entry : bucket) {
					if (entry.first == descriptor) {
						load = entry.second;
						break;
					}
				}
				if (!load) {
					load = make_shared_ptr<IcebergDeleteFileLoadState>();
					claimed_loads.push_back(load);
					bucket.emplace_back(descriptor, load);
					scan_entries.push_back({descriptor, load});
				}
				if (seen.insert(load.get()).second) {
					required_loads.push_back(load);
				}
			}
			if (scan_entries.size() > DELETE_FILES_PER_READ_TASK) {
				// The claimed loads become visible to other workers when the lock is released. Queue their read
				// tasks before that, so that a worker that finds one of them finds a task to run as well. During
				// a scan the regular threads are inside the scan and data files are opened on async threads, so
				// the tasks are queued where the async threads take them.
				read_tasks = make_shared_ptr<TaskExecutor>(context.context, TaskSchedulerType::ASYNC);
				vector<unique_ptr<BaseExecutorTask>> tasks;
				for (idx_t begin = 0; begin < scan_entries.size(); begin += DELETE_FILES_PER_READ_TASK) {
					const auto end = MinValue<idx_t>(begin + DELETE_FILES_PER_READ_TASK, scan_entries.size());
					vector<IcebergDeleteScanEntry> task_entries;
					for (idx_t i = begin; i < end; i++) {
						scan_entries[i].load->read_tasks = read_tasks;
						task_entries.push_back(std::move(scan_entries[i]));
					}
					tasks.push_back(
					    make_uniq<IcebergDeleteFileReadTask>(*read_tasks, context, std::move(task_entries)));
				}
				read_tasks->ScheduleTasks(std::move(tasks));
			}
		}
		// Read the delete files claimed by this worker before waiting for other builders. Its read tasks use
		// 'context', so they all have to be finished when this returns.
		if (read_tasks) {
			read_tasks->WorkOnTasks();
		} else if (!scan_entries.empty()) {
			// Few enough for one read: no task is queued.
			ReadDeleteFiles(context, scan_entries);
		}
	} catch (std::exception &ex) {
		// This worker leaves, and nothing else publishes what it claimed: no worker may wait for it.
		FailUnreadDeleteFileLoads(read_tasks.get(), claimed_loads, ErrorData(ex));
		throw;
	} catch (...) { // LCOV_EXCL_START
		FailUnreadDeleteFileLoads(read_tasks.get(), claimed_loads,
		                          ErrorData("Unknown exception while reading Iceberg delete files"));
		throw;
	} // LCOV_EXCL_STOP
	vector<shared_ptr<IcebergDeleteData>> positions;
	for (auto &load : required_loads) {
		unique_lock<mutex> guard(load->lock);
		// A delete file another worker claimed: run the read tasks of that claim until this file was read,
		// so that a worker blocked on delete files reads delete files. The task it takes need not hold this
		// file, so the worker can be held for one read task after the file was read.
		while (!load->complete) {
			auto other_tasks = load->read_tasks;
			if (!other_tasks) {
				break;
			}
			guard.unlock();
			shared_ptr<Task> task;
			const auto found = other_tasks->GetTask(task);
			if (found) {
				task->Execute(TaskExecutionMode::PROCESS_ALL);
			}
			task.reset();
			other_tasks.reset();
			guard.lock();
			if (!found) {
				break;
			}
		}
		load->cv.wait(guard, [&load] { return load->complete; });
		if (load->error.HasError()) {
			load->error.Throw();
		}
		if (load->equality_delete) {
			result.equality_deletes.emplace_back(*load->equality_delete);
		}
		auto entry = load->positional_deletes.find(data_file_path);
		if (entry != load->positional_deletes.end()) {
			positions.push_back(entry->second);
		}
	}
	// Assemble only this task's live, selected blobs. Cached contents remain immutable.
	// A live vector supersedes positional deletes, but two distinct live vectors for
	// the same data file are corrupt; invalidated vectors must be excluded by planning.
	shared_ptr<IcebergDeleteData> assembled;
	for (auto &position : positions) {
		if (position->type == IcebergDeleteType::DELETION_VECTOR) {
			if (assembled) {
				throw InvalidConfigurationException("Two deletion vectors reference the same data file in a scan task");
			}
			assembled = position;
		}
	}
	if (assembled) {
		// Older writers may have left file-scoped position deletes live alongside the vector.
		// The vector remains authoritative for rows, but its replacement also supersedes those files.
		// Copy only when adding sources: descriptor loads are immutable and may be shared by scan tasks.
		shared_ptr<IcebergDeletionVectorData> combined;
		for (auto &position : positions) {
			if (position->type != IcebergDeleteType::POSITIONAL_DELETE || position->source_files.empty()) {
				continue;
			}
			if (!combined) {
				combined = make_shared_ptr<IcebergDeletionVectorData>(
				    static_cast<const IcebergDeletionVectorData &>(*assembled));
			}
			combined->source_files.insert(combined->source_files.end(), position->source_files.begin(),
			                              position->source_files.end());
		}
		if (combined) {
			assembled = std::move(combined);
		}
	} else if (positions.size() == 1) {
		assembled = positions[0];
	} else if (!positions.empty()) {
		auto combined = make_shared_ptr<IcebergPositionalDeleteData>();
		for (auto &position : positions) {
			auto &source = static_cast<const IcebergPositionalDeleteData &>(*position);
			combined->MergeRows(source);
			combined->source_files.insert(combined->source_files.end(), source.source_files.begin(),
			                              source.source_files.end());
		}
		assembled = std::move(combined);
	}
	if (assembled) {
		result.positional_deletes = assembled->ToFilter();
		lock_guard<mutex> guard(lock);
		positional_delete_data[data_file_path] = std::move(assembled);
	}
	return result;
}

} // namespace duckdb
