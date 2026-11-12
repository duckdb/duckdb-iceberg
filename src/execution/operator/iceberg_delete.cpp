#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "execution/operator/iceberg_delete.hpp"
#include "execution/operator/iceberg_copy_on_write_delete.hpp"
#include "execution/operator/merge_into/iceberg_merge_delete_collector.hpp"

#include "iceberg_logging.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

#include "core/deletes/iceberg_deletion_vector.hpp"
#include "core/metadata/puffin/iceberg_puffin_metadata.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {
class IcebergDeleteLocalState;
class IcebergDeleteGlobalState;
class IcebergTableSchemaVersion;

static bool IsScanCreatedByDelete(const PhysicalTableScan &scan) {
	if (scan.function.GetName() != "iceberg_scan") {
		return false;
	}
	if (scan.column_ids.size() < 2) {
		return false;
	}

	bool has_file_name = false;
	bool has_file_row_number = false;
	for (auto &column : scan.column_ids) {
		if (!column.HasPrimaryIndex()) {
			continue;
		}
		auto index = column.GetPrimaryIndex();
		if (index == MultiFileReader::COLUMN_IDENTIFIER_FILENAME) {
			has_file_name = true;
		} else if (index == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			has_file_row_number = true;
		}
		if (has_file_name && has_file_row_number) {
			return true;
		}
	}
	return false;
}

optional_ptr<PhysicalTableScan> IcebergDelete::FindIcebergScan(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::MERGE_ACTION_SOURCE) {
		// the rows of a merge action are pushed into the source by the merge into - look for the scan in the plan
		// that the merge into reads from
		auto &merge_input = plan.Cast<PhysicalMergeActionSource>().merge_input;
		return merge_input ? FindIcebergScan(*merge_input) : nullptr;
	}
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		// does this emit the virtual columns?
		auto &scan = plan.Cast<PhysicalTableScan>();

		if (IsScanCreatedByDelete(scan)) {
			return scan;
		}
		return nullptr;
	}

	for (auto &children : plan.children) {
		auto result = FindIcebergScan(children.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

IcebergDelete::IcebergDelete(PhysicalPlan &physical_plan, IcebergTableSchemaVersion &table,
                             optional_ptr<IcebergMultiFileList> multi_file_list, PhysicalOperator &child,
                             vector<idx_t> row_id_indexes)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      multi_file_list(multi_file_list), row_id_indexes(std::move(row_id_indexes)) {
	children.push_back(child);
}

unique_ptr<GlobalSinkState> IcebergDelete::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<IcebergDeleteGlobalState>();
	if (merge_delete_action_index.IsValid()) {
		auto &transaction = IcebergTransaction::Get(context, table.catalog);
		auto &transaction_data = table.table_info.GetOrCreateTransactionData(transaction);
		result->merge_deletes = transaction_data.GetMergeDeleteCollector(merge_delete_action_count);
	}
	return std::move(result);
}

unique_ptr<LocalSinkState> IcebergDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<IcebergDeleteLocalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType IcebergDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();

	if (is_equality_delete) {
		//! The equality-delete file's contents come entirely from the planning-time predicates,
		//! not from the streamed rows - write it exactly once and stop consuming input.
		bool should_write = false;
		{
			lock_guard<mutex> guard(global_state.lock);
			if (!global_state.equality_delete_written) {
				global_state.equality_delete_written = true;
				should_write = true;
			}
		}
		if (should_write) {
			WriteEqualityDeleteFile(context.client, global_state);
		}
		return SinkResultType::FINISHED;
	}

	auto &local_state = input.local_state.Cast<IcebergDeleteLocalState>();

	auto &file_name_vector = chunk.data[row_id_indexes[0]];
	auto &file_row_number = chunk.data[row_id_indexes[1]];

	UnifiedVectorFormat row_data;
	file_row_number.ToUnifiedFormat(row_data);
	auto file_row_data = UnifiedVectorFormat::GetData<int64_t>(row_data);

	UnifiedVectorFormat file_name_vdata;
	file_name_vector.ToUnifiedFormat(file_name_vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto row_idx = row_data.sel->get_index(i);
		auto file_name_idx = file_name_vdata.sel->get_index(i);
		if (!file_name_vdata.validity.RowIsValid(file_name_idx)) {
			throw InternalException("Filename cannot be NULL!");
		}
		auto file_name_data = UnifiedVectorFormat::GetData<string_t>(file_name_vdata);
		auto file_name = file_name_data[file_name_idx].GetString();

		if (local_state.current_file_name.empty() || local_state.current_file_name != file_name) {
			// local_state points to new file, flush to global state
			global_state.Flush(local_state);
			local_state.current_file_name = file_name;
		}
		auto row_number = file_row_data[row_idx];
		local_state.file_row_numbers.push_back(row_number);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType IcebergDelete::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<IcebergDeleteLocalState>();
	global_state.FinalFlush(local_state);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

vector<data_t> IcebergDelete::SerializeDeletionVector(const set<idx_t> &sorted_deletes) const {
	// Build deletion vector data
	unordered_map<int32_t, roaring::Roaring> bitmaps;

	// Group row indices by high 32 bits
	for (auto row_idx : sorted_deletes) {
		int64_t row_id = static_cast<int64_t>(row_idx);
		int32_t high_bits = static_cast<int32_t>(row_id >> 32);
		uint32_t low_bits = static_cast<uint32_t>(row_id & 0xFFFFFFFF);

		auto &bitmap = bitmaps[high_bits];
		bitmap.add(low_bits);
	}

	return IcebergDeletionVectorData::ToBlob(bitmaps);
}

IcebergDeleteFileInfo IcebergDelete::WritePositionalDeleteFile(ClientContext &context, const string &filename,
                                                               IcebergDeleteFileInfo delete_file,
                                                               const set<idx_t> &sorted_deletes) const {
	auto delete_file_path = delete_file.file_name;
	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// generate the field ids to be written by the parquet writer
	// these field ids follow icebergs ids and names for the delete files
	child_list_t<Value> values;
	values.emplace_back("file_path", Value::INTEGER(MultiFileReader::DELETE_FILE_PATH_FIELD_ID));
	values.emplace_back("pos", Value::INTEGER(MultiFileReader::DELETE_POS_FIELD_ID));
	auto field_ids = Value::STRUCT(std::move(values));
	vector<Value> field_input;
	field_input.push_back(std::move(field_ids));
	info->options["field_ids"] = std::move(field_input);

	auto codec = table.table_info.GetMetadata().GetDeleteCompressionCodec();
	if (!codec.empty()) {
		info->options["codec"].emplace_back(codec);
	}

	vector<string> names_to_write {"file_path", "pos"};
	vector<LogicalType> types_to_write {LogicalType::VARCHAR, LogicalType::BIGINT};

	auto &copy_fun = IcebergUtils::GetCopyFunction(context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	auto function_data =
	    copy_fun.function.copy_to_bind(context, bind_input, StringsToIdentifiers(names_to_write), types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	// generate the physical copy to file
	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	PhysicalPlan plan(Allocator::Get(context));

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	// run the copy to file
	vector<LogicalType> write_types;
	write_types.push_back(LogicalType::VARCHAR);
	write_types.push_back(LogicalType::BIGINT);

	DataChunk write_chunk;
	write_chunk.Initialize(context, write_types);
	// the first vector is constant (the file name)
	Value filename_val(filename);
	write_chunk.data[0].Reference(filename_val, count_t(STANDARD_VECTOR_SIZE));

	idx_t row_count = 0;
	auto row_data = FlatVector::GetDataMutable<int64_t>(write_chunk.data[1]);
	for (auto &row_idx : sorted_deletes) {
		row_data[row_count++] = NumericCast<int64_t>(row_idx);
		if (row_count >= STANDARD_VECTOR_SIZE) {
			write_chunk.SetChildCardinality(row_count);
			copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
			                               write_chunk);
			row_count = 0;
		}
	}
	if (row_count > 0) {
		write_chunk.SetChildCardinality(row_count);
		copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
		                               write_chunk);
	}

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	delete_file.file_name = delete_file_path;
	delete_file.file_format = "parquet";
	delete_file.delete_count = stats.row_count;
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.footer_size = stats.footer_size_bytes.GetValue<idx_t>();
	auto pos_stats = stats.column_statistics.find("\"pos\"");
	auto pos_min = pos_stats->second.find("min");
	auto pos_min_value = pos_min->second.GetValue<idx_t>();
	auto pos_max = pos_stats->second.find("max");
	auto pos_max_value = pos_max->second.GetValue<idx_t>();
	delete_file.pos_min_value = pos_min_value;
	delete_file.pos_max_value = pos_max_value;
	DUCKDB_LOG(context, IcebergLogType,
	           "Iceberg DELETE, wrote positional_delete_file '%s' for data_file '%s', delete_count=%llu, "
	           "file_size=%llu bytes",
	           delete_file_path, filename, stats.row_count, stats.file_size_bytes);
	return delete_file;
}

static void PopulateAlteredManifests(IcebergManifestDeletes &out, const IcebergDeleteData &delete_data) {
	for (auto &file : delete_data.source_files) {
		out.InvalidateFile(file);
	}
}

namespace {

string DeleteFilePath(FileSystem &fs, const string &filename, const string &format) {
	// External writers can use forward slashes for Windows paths. Normalize only
	// the output directory; delete matching must retain the original data-file path.
	auto normalized_filename = fs.ConvertSeparators(filename);
	auto last_sep = normalized_filename.rfind(fs.PathSeparator(normalized_filename));
	if (last_sep == string::npos) {
		throw InvalidConfigurationException("Cannot create valid file path for delete file");
	}
	// A shared Puffin file lives beside its first data file. Each blob retains its own partition in the manifest.
	return fs.JoinPath(normalized_filename.substr(0, last_sep + 1),
	                   UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes." + format);
}

idx_t DeleteTargetFileSize(const IcebergTableMetadata &metadata) {
	constexpr const char *PROPERTY = "write.delete.target-file-size-bytes";
	auto &properties = metadata.GetTableProperties();
	auto entry = properties.find(PROPERTY);
	if (entry == properties.end()) {
		return 64ULL * 1024 * 1024;
	}
	int64_t size;
	if (!TryCast::Operation<string_t, int64_t>(string_t(entry->second), size, true) || size <= 0) {
		throw InvalidInputException("Iceberg: '%s' must be a positive integer number of bytes, got '%s'", PROPERTY,
		                            entry->second);
	}
	return static_cast<idx_t>(size);
}

IcebergPuffinBlobMetadata DeletionVectorMetadata(const IcebergDeleteFileInfo &file) {
	IcebergPuffinBlobMetadata metadata;
	metadata.type = "deletion-vector-v1";
	metadata.fields.push_back(MultiFileReader::DELETE_POS_FIELD_ID);
	metadata.snapshot_id = -1;
	metadata.sequence_number = -1;
	metadata.properties = case_insensitive_map_t<string> {{"referenced-data-file", file.data_file_path},
	                                                      {"cardinality", std::to_string(file.delete_count)}};
	return metadata;
}

void FinishDeleteFiles(const IcebergDelete &op, ClientContext &context, IcebergDeleteGlobalState &global_state,
                       vector<IcebergDeleteFileWrite> &writes, bool stage_snapshot) {
	{
		lock_guard<mutex> guard(global_state.lock);
		auto &result = global_state.write_result;
		for (auto &write : writes) {
			if (write.superseded_deletes) {
				PopulateAlteredManifests(result.altered_manifests, *write.superseded_deletes);
			}
			auto data_file_path = write.delete_file.data_file_path;
			result.written_files.emplace(std::move(data_file_path), std::move(write.delete_file));
		}
	}
	if (stage_snapshot) {
		op.AddDeleteSnapshot(context, global_state);
	}
}

//! Each write task owns one size-bounded Puffin file.
class IcebergDeletionVectorWriteTask : public ExecutorTask {
public:
	IcebergDeletionVectorWriteTask(Executor &executor, shared_ptr<Event> event,
	                               vector<reference<IcebergDeleteFileWrite>> inputs)
	    : ExecutorTask(executor, std::move(event)), inputs(std::move(inputs)) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		auto &context = event->GetClientContext();
		context.InterruptCheck();
		auto &fs = FileSystem::GetFileSystem(context);
		IcebergPuffinWriter writer(fs, DeleteFilePath(fs, inputs[0].get().delete_file.data_file_path, "puffin"));
		for (auto &entry : inputs) {
			context.InterruptCheck();
			auto &input = entry.get();
			auto &file = input.delete_file;
			file.file_name = writer.GetPath();
			file.content_offset = writer.WriteBlob(input.deletion_vector, DeletionVectorMetadata(file));
			input.deletion_vector = vector<data_t>();
		}
		auto file_size = writer.Finish();
		for (auto &input : inputs) {
			input.get().delete_file.file_size_bytes = file_size;
		}
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg DELETE, wrote deletion_vector_file '%s', vector_count=%llu, file_size=%llu bytes",
		           writer.GetPath(), inputs.size(), file_size);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "IcebergDeletionVectorWriteTask";
	}

private:
	vector<reference<IcebergDeleteFileWrite>> inputs;
};

class IcebergDeletionVectorWriteEvent : public BasePipelineEvent {
public:
	IcebergDeletionVectorWriteEvent(Pipeline &pipeline, const IcebergDelete &op, IcebergDeleteGlobalState &global_state,
	                                vector<IcebergDeleteFileWrite> writes, bool stage_snapshot, idx_t target_file_size)
	    : BasePipelineEvent(pipeline), op(op), global_state(global_state), writes(std::move(writes)),
	      stage_snapshot(stage_snapshot), target_file_size(target_file_size) {
	}

	void Schedule() override {
		// DVs are always per data file. Unlike v2 position deletes, write.delete.granularity does not
		// constrain their containers: the v3 spec permits Puffin files to span partitions and specs.
		vector<shared_ptr<Task>> tasks;
		vector<reference<IcebergDeleteFileWrite>> inputs;
		IcebergPuffinFileLayout layout;
		for (auto &write : writes) {
			auto next_layout = layout;
			auto metadata = DeletionVectorMetadata(write.delete_file);
			auto size = next_layout.AddBlob(write.deletion_vector.size(), metadata);
			if (!inputs.empty() && size > target_file_size) {
				tasks.push_back(
				    make_shared_ptr<IcebergDeletionVectorWriteTask>(executor, shared_from_this(), std::move(inputs)));
				inputs = vector<reference<IcebergDeleteFileWrite>>();
				next_layout = IcebergPuffinFileLayout();
				next_layout.AddBlob(write.deletion_vector.size(), std::move(metadata));
			}
			// An oversized vector must stay intact: a data file can only have one live DV.
			inputs.push_back(write);
			layout = next_layout;
		}
		tasks.push_back(
		    make_shared_ptr<IcebergDeletionVectorWriteTask>(executor, shared_from_this(), std::move(inputs)));
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		FinishDeleteFiles(op, GetClientContext(), global_state, writes, stage_snapshot);
	}

private:
	const IcebergDelete &op;
	IcebergDeleteGlobalState &global_state;
	vector<IcebergDeleteFileWrite> writes;
	bool stage_snapshot;
	idx_t target_file_size;
};

//! Merge old deletes and serialize each vector in parallel before packing by actual encoded size.
class IcebergDeleteFileMaterializeTask : public ExecutorTask {
public:
	IcebergDeleteFileMaterializeTask(Executor &executor, shared_ptr<Event> event, const IcebergDelete &op,
	                                 const string &filename, vector<idx_t> &deleted_rows, IcebergDeleteFileWrite &out)
	    : ExecutorTask(executor, std::move(event)), op(op), filename(filename), deleted_rows(deleted_rows), out(out) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		auto &context = event->GetClientContext();
		context.InterruptCheck();
		out = op.MaterializeDeletes(context, filename, deleted_rows);
		// The encoded vector replaces the pending row positions while waiting for the write event.
		deleted_rows = vector<idx_t>();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "IcebergDeleteFileMaterializeTask";
	}

private:
	const IcebergDelete &op;
	const string &filename;
	vector<idx_t> &deleted_rows;
	IcebergDeleteFileWrite &out;
};

class IcebergDeleteFileMaterializeEvent : public BasePipelineEvent {
public:
	IcebergDeleteFileMaterializeEvent(Pipeline &pipeline, const IcebergDelete &op,
	                                  IcebergDeleteGlobalState &global_state, IcebergPendingDeletes pending_deletes,
	                                  bool stage_snapshot)
	    : BasePipelineEvent(pipeline), op(op), global_state(global_state), pending_deletes(std::move(pending_deletes)),
	      writes(this->pending_deletes.rows_by_file.size()), stage_snapshot(stage_snapshot) {
	}

	void Schedule() override {
		if (op.table.table_info.GetMetadata().iceberg_version >= 3) {
			target_file_size = DeleteTargetFileSize(op.table.table_info.GetMetadata());
		}
		vector<shared_ptr<Task>> tasks;
		idx_t index = 0;
		for (auto &entry : pending_deletes.rows_by_file) {
			tasks.push_back(make_shared_ptr<IcebergDeleteFileMaterializeTask>(
			    executor, shared_from_this(), op, entry.first, entry.second, writes[index++]));
		}
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		if (op.table.table_info.GetMetadata().iceberg_version >= 3) {
			InsertEvent(make_shared_ptr<IcebergDeletionVectorWriteEvent>(*pipeline, op, global_state, std::move(writes),
			                                                             stage_snapshot, target_file_size));
		} else {
			FinishDeleteFiles(op, GetClientContext(), global_state, writes, stage_snapshot);
		}
	}

private:
	const IcebergDelete &op;
	IcebergDeleteGlobalState &global_state;
	IcebergPendingDeletes pending_deletes;
	vector<IcebergDeleteFileWrite> writes;
	bool stage_snapshot;
	idx_t target_file_size = 0;
};

} // namespace

void IcebergDelete::ScheduleDeletes(Pipeline &pipeline, Event &event, IcebergDeleteGlobalState &global_state,
                                    bool stage_snapshot) const {
	D_ASSERT(!is_equality_delete);
	auto pending_deletes = global_state.TakePendingDeletes();
	if (global_state.merge_deletes) {
		D_ASSERT(stage_snapshot);
		// Earlier actions must finish so that the remaining contributors can run.
		auto merged_deletes = global_state.merge_deletes->CompleteAction(merge_delete_action_index.GetIndex(),
		                                                                 std::move(pending_deletes));
		if (!merged_deletes) {
			return;
		}
		pending_deletes = std::move(*merged_deletes);
	}
	if (pending_deletes.IsEmpty()) {
		// An empty UPDATE may have no source scan after optimization.
		return;
	}
	if (!multi_file_list) {
		throw InternalException("IcebergDelete multi_file_list is NULL");
	}
	event.InsertEvent(make_shared_ptr<IcebergDeleteFileMaterializeEvent>(pipeline, *this, global_state,
	                                                                     std::move(pending_deletes), stage_snapshot));
}

IcebergDeleteFileWrite IcebergDelete::MaterializeDeletes(ClientContext &context, const string &filename,
                                                         const vector<idx_t> &deleted_rows) const {
	bool write_deletion_vector = table.table_info.GetMetadata().iceberg_version >= 3;
	IcebergDeleteFileWrite result;

	// Check newly selected positions before adding existing deletes: only new duplicate matches are errors.
	set<idx_t> sorted_deletes;
	for (auto &row_idx : deleted_rows) {
		sorted_deletes.insert(row_idx);
	}
	if (sorted_deletes.size() != deleted_rows.size()) {
		throw NotImplementedException("The same row was updated multiple times - this is not (yet) supported in "
		                              "Iceberg. Eliminate duplicate matches prior to running the UPDATE");
	}
	if (write_deletion_vector) {
		// A v3 vector must also retain all positions already deleted from this data file.
		result.superseded_deletes = multi_file_list->GetExistingPositionalDeleteData(filename);
		if (result.superseded_deletes) {
			result.superseded_deletes->ToSet(sorted_deletes);
		}
	}

	IcebergDeleteFileInfo delete_file;
	delete_file.data_file_path = filename;
	delete_file.partition_info = multi_file_list->GetScanPlanner().GetPartitionForDataFile(filename);

	if (!write_deletion_vector) {
		delete_file.file_name = DeleteFilePath(FileSystem::GetFileSystem(context), filename, "parquet");
		result.delete_file = WritePositionalDeleteFile(context, filename, std::move(delete_file), sorted_deletes);
	} else {
		result.deletion_vector = SerializeDeletionVector(sorted_deletes);
		delete_file.file_format = "puffin";
		delete_file.delete_count = sorted_deletes.size();
		delete_file.content_size_in_bytes = result.deletion_vector.size();
		result.delete_file = std::move(delete_file);
	}
	return result;
}

partitioned_manifest_entry_map_t IcebergDelete::GenerateDeleteManifestEntries(IcebergDeleteGlobalState &global_state) {
	lock_guard<mutex> guard(global_state.lock);
	return global_state.write_result.GenerateManifestEntries();
}

partitioned_manifest_entry_map_t IcebergDeleteWriteResult::GenerateManifestEntries() const {
	partitioned_manifest_entry_map_t iceberg_delete_files;
	for (const auto &delete_entry : written_files) {
		auto data_file_name = delete_entry.first;
		auto &delete_file = delete_entry.second;

		IcebergManifestEntry manifest_entry;
		manifest_entry.status = IcebergManifestEntryStatusType::ADDED;
		auto &data_file = manifest_entry.data_file;

		// This will only ever be triggered when duckdb-iceberg
		// is built with ICEBERG_ENABLE_EQUALITY_DELETE_WRITES=1
		if (!delete_file.equality_ids.empty()) {
			//! Equality delete: a global delete identified only by the equality field values.
			//! It has no referenced data file, no filename bounds and no partition info.
			data_file.content = IcebergManifestEntryContentType::EQUALITY_DELETES;
			data_file.file_path = delete_file.file_name;
			data_file.file_format = delete_file.file_format;
			data_file.record_count = delete_file.delete_count;
			data_file.file_size_in_bytes = delete_file.file_size_bytes;
			data_file.equality_ids = delete_file.equality_ids;
			//! Record the equality-delete metrics so the file can be pruned safely.
			data_file.lower_bounds = delete_file.lower_bounds;
			data_file.upper_bounds = delete_file.upper_bounds;
			data_file.null_value_counts = delete_file.null_value_counts;
			data_file.nan_value_counts = delete_file.nan_value_counts;
			iceberg_delete_files[delete_file.partition_info.partition_spec_id].push_back(manifest_entry);
			continue;
		}

		data_file.content = IcebergManifestEntryContentType::POSITION_DELETES;
		data_file.file_path = delete_file.file_name;
		data_file.file_format = delete_file.file_format;
		data_file.record_count = delete_file.delete_count;
		data_file.file_size_in_bytes = delete_file.file_size_bytes;
		if (delete_file.content_size_in_bytes.IsValid()) {
			data_file.content_size_in_bytes = delete_file.content_size_in_bytes.GetIndex();
		}
		if (delete_file.content_offset.IsValid()) {
			data_file.content_offset = delete_file.content_offset.GetIndex();
		}

		// Filename bounds contain raw UTF-8 bytes, not escaped BLOB literals.
		data_file.lower_bounds[MultiFileReader::DELETE_FILE_PATH_FIELD_ID] = Value::BLOB_RAW(data_file_name);
		data_file.upper_bounds[MultiFileReader::DELETE_FILE_PATH_FIELD_ID] = Value::BLOB_RAW(data_file_name);
		// set referenced_data_file
		data_file.referenced_data_file = data_file_name;
		// copy partition info from the data file being deleted
		auto &partition = delete_file.partition_info;
		data_file.partition_info = partition.fields;
		//! Keyed by the data file's spec, not the table's current one: the values above are expressed in that
		//! spec, and a reader only applies a delete whose partition (spec included) equals the data file's.
		iceberg_delete_files[partition.partition_spec_id].push_back(manifest_entry);
	}
	return iceberg_delete_files;
}

SinkFinalizeType IcebergDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();

	if (!is_equality_delete) {
		ScheduleDeletes(pipeline, event, global_state, true);
		return SinkFinalizeType::READY;
	}

	//! Ensure the equality-delete file is written even if Sink never ran (e.g. zero matching rows).
	bool should_write = false;
	{
		lock_guard<mutex> guard(global_state.lock);
		if (!global_state.equality_delete_written) {
			global_state.equality_delete_written = true;
			should_write = true;
		}
	}
	if (should_write) {
		WriteEqualityDeleteFile(context, global_state);
	}
	AddDeleteSnapshot(context, global_state);
	return SinkFinalizeType::READY;
}

void IcebergDelete::AddDeleteSnapshot(ClientContext &context, IcebergDeleteGlobalState &global_state) const {
	auto &iceberg_transaction = IcebergTransaction::Get(context, table.catalog);
	auto iceberg_delete_files = GenerateDeleteManifestEntries(global_state);
	if (!global_state.write_result.written_files.empty()) {
		ApplyTableUpdate(table.table_info, iceberg_transaction, [&](IcebergTable &tbl) {
			auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
			transaction_data.AddDeleteSnapshot(std::move(iceberg_delete_files),
			                                   std::move(global_state.write_result.altered_manifests), merge_action);
		});
	}
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergDeleteGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_deleted_count.load()));
	chunk.data[0].Append(value);
	chunk.SetChildCardinality(1);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string IcebergDelete::GetName() const {
	return "ICEBERG_DELETE";
}

InsertionOrderPreservingMap<string> IcebergDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name.GetIdentifierName();
	return result;
}

PhysicalOperator &IcebergDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                            IcebergTableSchemaVersion &table, PhysicalOperator &child_plan,
                                            vector<idx_t> &&row_id_indexes) {
	// DELETE, UPDATE and MERGE all plan their delete files here.
	table.table_info.GetMetadata().VerifyDeleteFileFormat();
	auto scan = FindIcebergScan(child_plan);

	optional_ptr<IcebergMultiFileList> multi_file_list;
	if (scan) {
		auto &bind_data = scan->bind_data->Cast<MultiFileBindData>();
		multi_file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
	} else {
		DUCKDB_LOG_DEBUG(context, "Could not find IcebergDelete source. Iceberg Multi File list is empty");
	}

#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	vector<IcebergEqualityDeletePredicate> equality_predicates;
	// MERGE deletes only the rows selected by its action conditions, not a broader target-scan predicate.
	bool is_equality_delete = !IcebergTransaction::Get(context, table.catalog).planning_merge_into &&
	                          TryGetEqualityDeletePredicates(context, table, child_plan, equality_predicates);
	return planner.Make<IcebergDelete>(table, multi_file_list, child_plan, std::move(row_id_indexes),
	                                   is_equality_delete, std::move(equality_predicates));
#else
	return planner.Make<IcebergDelete>(table, multi_file_list, child_plan, std::move(row_id_indexes));
#endif
}

PhysicalOperator &IcebergCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalDelete &op) {
	auto &irc_transaction = IcebergTransaction::Get(context, *this);
	//! A MERGE plans its delete action through PlanDeleteOperation, which applies write.merge.mode instead
	if (!op.return_chunk && !irc_transaction.planning_merge_into) {
		auto &table_entry = op.table.Cast<IcebergTableSchemaVersion>();
		auto &alter = irc_transaction.GetOrCreateAlter();
		auto &updated_table = alter.GetOrInitializeTable(table_entry.table_info);
		auto &table_metadata = updated_table.GetMetadata();
		//! V3 is not supported yet: a rewrite has to carry each kept row's lineage into the new files. A V3 DELETE
		//! continues below, where a copy-on-write mode is refused as before.
		if (table_metadata.iceberg_version == 2 && table_metadata.UsesCopyOnWrite(WRITE_DELETE_MODE)) {
			table_entry.table_info.VerifyWriteEncryptionSupported();
			table_entry.PrepareIcebergScanFromEntry(context);
			auto &updated_table_entry = *updated_table.schema_versions[table_metadata.GetLatestSchema().schema_id];
			return IcebergCopyOnWriteDeleteFilter::PlanDelete(context, planner, op, updated_table_entry);
		}
	}
	return Catalog::PlanDelete(context, planner, op);
}

PhysicalOperator &IcebergCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	return PlanDeleteOperation(context, planner, op, plan);
}

PhysicalOperator &IcebergCatalog::PlanDeleteOperation(ClientContext &context, PhysicalPlanGenerator &planner,
                                                      LogicalDelete &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion from Iceberg table");
	}
	auto &table_entry = op.table.Cast<IcebergTableSchemaVersion>();
	table_entry.table_info.VerifyWriteEncryptionSupported();
	table_entry.PrepareIcebergScanFromEntry(context);

	auto &irc_transaction = IcebergTransaction::Get(context, *this);
	auto &alter = irc_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_entry.table_info);
	auto &table_metadata = updated_table.GetMetadata();
	auto &schema = table_metadata.GetLatestSchema();
	auto &updated_table_entry = *updated_table.schema_versions[schema.schema_id];

	auto &metadata = updated_table_entry.table_info.GetMetadata();
	auto iceberg_version = metadata.iceberg_version;
	if (iceberg_version < 2) {
		throw NotImplementedException("Delete from Iceberg V%d tables", metadata.iceberg_version);
	}

	vector<idx_t> row_id_indexes;
	//! Positional deletes use filename and file row number, followed by unused lineage in V3.
	for (idx_t i = 0; i < 2; i++) {
		auto &bound_ref = op.expressions[i]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(bound_ref.Index());
	}

	if (!irc_transaction.planning_merge_into) {
		VerifyMergeOnRead(metadata, updated_table_entry.name.GetIdentifierName(), WRITE_DELETE_MODE);
	}

	auto &iceberg_delete =
	    IcebergDelete::PlanDelete(context, planner, updated_table_entry, plan, std::move(row_id_indexes));
	return iceberg_delete;
}

} // namespace duckdb
