#include "../include/storage/iceberg_delete.hpp"
#include "storage/iceberg_delete.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/iceberg_table_information.hpp"
#include "iceberg_multi_file_reader.hpp"
#include "iceberg_multi_file_list.hpp"
#include "storage/irc_transaction.hpp"

#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {
class IcebergDeleteLocalState;
class IcebergDeleteGlobalState;
class ICTableEntry;

IcebergDelete::IcebergDelete(PhysicalPlan &physical_plan, ICTableEntry &table, PhysicalOperator &child,
                             shared_ptr<IcebergDeleteMap> delete_map_p, vector<idx_t> row_id_indexes)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      delete_map(std::move(delete_map_p)), row_id_indexes(std::move(row_id_indexes)) {
	children.push_back(child);
}

unique_ptr<GlobalSinkState> IcebergDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergDeleteGlobalState>();
}

unique_ptr<LocalSinkState> IcebergDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<IcebergDeleteLocalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType IcebergDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<IcebergDeleteLocalState>();

	auto &file_name_vector = chunk.data[row_id_indexes[0]];
	auto &file_index_vector = chunk.data[row_id_indexes[1]];
	auto &file_row_number = chunk.data[row_id_indexes[2]];

	UnifiedVectorFormat row_data;
	file_row_number.ToUnifiedFormat(chunk.size(), row_data);
	auto file_row_data = UnifiedVectorFormat::GetData<int64_t>(row_data);

	UnifiedVectorFormat file_name_vdata;
	file_name_vector.ToUnifiedFormat(chunk.size(), file_name_vdata);

	UnifiedVectorFormat file_index_vdata;
	file_index_vector.ToUnifiedFormat(chunk.size(), file_index_vdata);

	auto file_index_data = UnifiedVectorFormat::GetData<uint64_t>(file_index_vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto file_idx = file_index_vdata.sel->get_index(i);
		auto row_idx = row_data.sel->get_index(i);
		if (!file_index_vdata.validity.RowIsValid(file_idx)) {
			throw InternalException("File index cannot be NULL!");
		}
		auto file_index = file_index_data[file_idx];
		if (!local_state.current_file_index.IsValid() || file_index != local_state.current_file_index.GetIndex()) {
			// file has changed - flush
			global_state.Flush(local_state);
			local_state.current_file_index = file_index;
			// insert the file name for the file if it has not yet been inserted
			auto entry = local_state.filenames.find(file_index);
			if (entry == local_state.filenames.end()) {
				auto file_name_idx = file_name_vdata.sel->get_index(i);
				auto file_name_data = UnifiedVectorFormat::GetData<string_t>(file_name_vdata);
				if (!file_name_vdata.validity.RowIsValid(file_name_idx)) {
					throw InternalException("Filename cannot be NULL!");
				}
				local_state.filenames.emplace(file_index, file_name_data[file_name_idx].GetString());
			}
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

static optional_ptr<CopyFunctionCatalogEntry> TryGetCopyFunction(DatabaseInstance &db, const string &name) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	return schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, name)->Cast<CopyFunctionCatalogEntry>();
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
void IcebergDelete::FlushDelete(IRCTransaction &transaction, ClientContext &context,
                                IcebergDeleteGlobalState &global_state, const string &filename,
                                vector<idx_t> &deleted_rows) const {
	// find the matching data file for the deletion
	auto data_file_info = delete_map->GetExtendedFileInfo(filename);

	// sort and duplicate eliminate the deletes
	set<idx_t> sorted_deletes;
	for (auto &row_idx : deleted_rows) {
		sorted_deletes.insert(row_idx);
	}
	if (sorted_deletes.size() != deleted_rows.size()) {
		throw NotImplementedException("The same row was updated multiple times - this is not (yet) supported in "
		                              "Iceberg. Eliminate duplicate matches prior to running the UPDATE");
	}

	IcebergDeleteFileInfo delete_file;
	delete_file.data_file_path = filename;
	delete_file.data_file_id = data_file_info.file_id;
	// check if the file already has deletes
	auto existing_delete_data = delete_map->GetDeleteData(filename);
	// TODO: for V3 deletes, we need to be able to find existing delete files and merge with them
	D_ASSERT(!existing_delete_data);
	if (sorted_deletes.size() == data_file_info.row_count) {
		// ALL rows in this file are deleted - we don't need to write the deletes out to a file
		// we can just invalidate the source data file directly
		if (delete_file.data_file_id.IsValid()) {
			// persistent file - drop the file as part of the transaction
			// TODO: transaction does not have a DropFile command
			//  but the concept still stands that we can rewrite the manifest to not include a completely deleted file.
			// transaction.DropFile(table.GetTableId(), delete_file.data_file_id, data_file_info.file.path);
		}
		return;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	// TODO: figure out how to get a UUID filepattern here.
	// auto delete_file_uuid = transaction.GenerateUUID() + "-delete.parquet";
	string delete_file_uuid = UUID::ToString(UUID::GenerateRandomUUID()) + "-deletes.parquet";
	string delete_file_path =
	    fs.JoinPath(table.table_info.table_metadata.location, fs.JoinPath("data", delete_file_uuid));

	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// generate the field ids to be written by the parquet writer
	// these field ids follow icebergs' ids and names for the delete files
	child_list_t<Value> values;
	// TODO: once https://github.com/duckdb/duckdb/pull/18617 is merged, used values
	// in multifile reader
	values.emplace_back("file_path", Value::INTEGER(2147483546));
	values.emplace_back("pos", Value::INTEGER(2147483545));
	auto field_ids = Value::STRUCT(std::move(values));
	vector<Value> field_input;
	field_input.push_back(std::move(field_ids));
	info->options["field_ids"] = std::move(field_input);

	// get the actual copy function and bind it
	auto copy_fun = TryGetCopyFunction(*context.db, "parquet");

	CopyFunctionBindInput bind_input(*info);

	vector<string> names_to_write {"file_path", "pos"};
	vector<LogicalType> types_to_write {LogicalType::VARCHAR, LogicalType::BIGINT};

	auto function_data = copy_fun->function.copy_to_bind(context, bind_input, names_to_write, types_to_write);

	// generate the physical copy to file
	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	PhysicalPlan plan(Allocator::Get(context));
	PhysicalCopyToFile copy_to_file(plan, copy_return_types, copy_fun->function, std::move(function_data), 1);

	copy_to_file.use_tmp_file = false;
	copy_to_file.file_path = delete_file_path;
	copy_to_file.partition_output = false;
	copy_to_file.write_empty_file = false;
	copy_to_file.file_extension = "parquet";
	copy_to_file.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	copy_to_file.per_thread_output = false;
	copy_to_file.rotate = false;
	copy_to_file.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	copy_to_file.write_partition_columns = false;
	copy_to_file.expected_types = {LogicalType::VARCHAR, LogicalType::BIGINT};

	// run the copy to file
	vector<LogicalType> write_types;
	write_types.push_back(LogicalType::VARCHAR);
	write_types.push_back(LogicalType::BIGINT);

	DataChunk write_chunk;
	write_chunk.Initialize(context, write_types);

	// the first vector is constant (the file name)
	Value filename_val(filename);
	write_chunk.data[0].Reference(filename_val);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	InterruptState interrupt_state;

	// run the PhysicalCopyToFile Sink pipeline
	auto gstate = copy_to_file.GetGlobalSinkState(context);
	auto lstate = copy_to_file.GetLocalSinkState(execution_context);

	OperatorSinkInput sink_input {*gstate, *lstate, interrupt_state};
	idx_t row_count = 0;
	auto row_data = FlatVector::GetData<int64_t>(write_chunk.data[1]);
	for (auto &row_idx : sorted_deletes) {
		row_data[row_count++] = NumericCast<int64_t>(row_idx);
		if (row_count >= STANDARD_VECTOR_SIZE) {
			write_chunk.SetCardinality(row_count);
			copy_to_file.Sink(execution_context, write_chunk, sink_input);
			row_count = 0;
		}
	}
	if (row_count > 0) {
		write_chunk.SetCardinality(row_count);
		copy_to_file.Sink(execution_context, write_chunk, sink_input);
	}
	OperatorSinkCombineInput combine_input {*gstate, *lstate, interrupt_state};
	copy_to_file.Combine(execution_context, combine_input);
	copy_to_file.FinalizeInternal(context, *gstate);

	// now read the stats data
	copy_to_file.sink_state = std::move(gstate);
	auto source_state = copy_to_file.GetGlobalSourceState(context);
	auto local_state = copy_to_file.GetLocalSourceState(execution_context, *source_state);
	DataChunk stats_chunk;
	stats_chunk.Initialize(context, copy_to_file.types);

	OperatorSourceInput source_input {*source_state, *local_state, interrupt_state};
	copy_to_file.GetData(execution_context, stats_chunk, source_input);

	if (stats_chunk.size() != 1) {
		throw InternalException("Expected a single delete file to be written here");
	}
	idx_t r = 0;
	delete_file.file_name = stats_chunk.GetValue(0, r).GetValue<string>();
	delete_file.delete_count = stats_chunk.GetValue(1, r).GetValue<idx_t>();
	delete_file.file_size_bytes = stats_chunk.GetValue(2, r).GetValue<idx_t>();
	delete_file.footer_size = stats_chunk.GetValue(3, r).GetValue<idx_t>();
	// TODO: add this back in
	global_state.written_files.emplace(filename, std::move(delete_file));
}

SinkFinalizeType IcebergDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergDeleteGlobalState>();
	if (global_state.deleted_rows.empty()) {
		return SinkFinalizeType::READY;
	}

	auto &irc_transaction = IRCTransaction::Get(context, table.catalog);
	// write out the delete rows
	for (auto &entry : global_state.deleted_rows) {
		auto filename_entry = global_state.filenames.find(entry.first);
		if (filename_entry == global_state.filenames.end()) {
			throw InternalException("Filename not found for file index");
		}
		FlushDelete(irc_transaction, context, global_state, filename_entry->second, entry.second);
	}
	auto &irc_table = table.Cast<ICTableEntry>();
	auto &table_info = irc_table.table_info;
	auto &transaction = IRCTransaction::Get(context, table.catalog);
	vector<IcebergManifestEntry> iceberg_delete_files;
	for (auto &entry : global_state.written_files) {
		auto data_file_name = entry.first;
		auto &delete_file = entry.second;
		IcebergManifestEntry manifest_entry;
		manifest_entry.status = IcebergManifestEntryStatusType::ADDED;
		manifest_entry.content = IcebergManifestEntryContentType::POSITION_DELETES;
		manifest_entry.file_path = delete_file.file_name;
		manifest_entry.file_format = "parquet";
		manifest_entry.record_count = delete_file.delete_count;
		manifest_entry.file_size_in_bytes = delete_file.file_size_bytes;

		// set lower and upper bound for the filename column
		manifest_entry.lower_bounds[2147483546] = Value::BLOB(data_file_name);
		manifest_entry.upper_bounds[2147483546] = Value::BLOB(data_file_name);
		// set referenced_data_file
		manifest_entry.referenced_data_file = data_file_name;

		iceberg_delete_files.push_back(std::move(manifest_entry));
	}
	// TODO: add a Delete update to the transaction.
	table_info.AddDeleteSnapshot(transaction, std::move(iceberg_delete_files));
	transaction.MarkTableAsDirty(irc_table);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergDelete::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergDeleteGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_deleted_count));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
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
	result["Table Name"] = table.name;
	return result;
}

optional_ptr<PhysicalTableScan> FindIcebergDeleteSource(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		// does this emit the virtual columns?
		auto &scan = plan.Cast<PhysicalTableScan>();
		bool found = false;
		for (auto &col : scan.column_ids) {
			if (col.GetPrimaryIndex() == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
				found = true;
				break;
			}
		}
		if (!found) {
			return nullptr;
		}
		return scan;
	}
	for (auto &children : plan.children) {
		auto result = FindIcebergDeleteSource(children.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

PhysicalOperator &IcebergDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, ICTableEntry &table,
                                            PhysicalOperator &child_plan, vector<idx_t> row_id_indexes) {
	auto delete_source = FindIcebergDeleteSource(child_plan);
	auto delete_map = make_shared_ptr<IcebergDeleteMap>();
	if (delete_source) {
		auto &bind_data = delete_source->bind_data->Cast<MultiFileBindData>();
		auto &reader = bind_data.multi_file_reader->Cast<IcebergMultiFileReader>();
		auto &file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
		auto files = file_list.GetFilesExtended();
		for (auto &file_entry : files) {
			delete_map->AddExtendedFileInfo(std::move(file_entry));
		}
		reader.delete_map = delete_map;
	}
	return planner.Make<IcebergDelete>(table, child_plan, std::move(delete_map), std::move(row_id_indexes));
}

PhysicalOperator &IRCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                        PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion from Iceberg table");
	}

	vector<idx_t> row_id_indexes;
	for (idx_t i = 0; i < 3; i++) {
		auto &bound_ref = op.expressions[i + 1]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(bound_ref.index);
	}
	auto &ic_table_entry = op.table.Cast<ICTableEntry>();
	auto &iceberg_delete = IcebergDelete::PlanDelete(context, planner, ic_table_entry, plan, row_id_indexes);
	return iceberg_delete;
}

} // namespace duckdb
