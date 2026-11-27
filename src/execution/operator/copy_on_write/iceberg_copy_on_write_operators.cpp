#include "execution/operator/copy_on_write/iceberg_copy_on_write_operators.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "core/deletes/iceberg_delete_data.hpp"
#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Row counter
//===--------------------------------------------------------------------===//
namespace {

class RowCounterGlobalState : public GlobalOperatorState {
public:
	atomic<idx_t> count {0};
	mutex lock;
	unordered_set<string> files;
};

class RowCounterLocalState : public OperatorState {
public:
	//! The files this thread has recorded, so the global lock is only taken for a new one
	unordered_set<string> seen_files;
	//! The scan reads one file at a time, so the last file is usually the next one
	bool has_last_file = false;
	string last_file;
};

void RecordFile(RowCounterGlobalState &gstate, RowCounterLocalState &state, const string_t &file_name) {
	if (state.has_last_file && file_name == string_t(state.last_file)) {
		return;
	}
	auto path = file_name.GetString();
	state.has_last_file = true;
	state.last_file = path;
	if (state.seen_files.insert(path).second) {
		lock_guard<mutex> guard(gstate.lock);
		gstate.files.insert(std::move(path));
	}
}

} // namespace

IcebergCopyOnWriteRowCounter::IcebergCopyOnWriteRowCounter(PhysicalPlan &physical_plan, PhysicalOperator &child,
                                                           optional_idx file_column)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.GetTypes(), child.estimated_cardinality),
      file_column(file_column) {
	children.push_back(child);
}

unique_ptr<GlobalOperatorState> IcebergCopyOnWriteRowCounter::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<RowCounterGlobalState>();
}

unique_ptr<OperatorState> IcebergCopyOnWriteRowCounter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<RowCounterLocalState>();
}

OperatorResultType IcebergCopyOnWriteRowCounter::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                         GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<RowCounterGlobalState>();
	gstate.count += input.size();
	if (file_column.IsValid() && input.size() > 0) {
		auto &state = state_p.Cast<RowCounterLocalState>();
		auto &file_name_vector = input.data[file_column.GetIndex()];
		if (file_name_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			//! The scan emits one file per chunk as a constant: record it once, not for every row
			if (ConstantVector::IsNull(file_name_vector)) {
				throw InternalException("Filename cannot be NULL!");
			}
			RecordFile(gstate, state, ConstantVector::GetData<string_t>(file_name_vector)[0]);
		} else {
			UnifiedVectorFormat file_name_data;
			file_name_vector.ToUnifiedFormat(file_name_data);
			auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_data);
			for (idx_t i = 0; i < input.size(); i++) {
				auto file_idx = file_name_data.sel->get_index(i);
				if (!file_name_data.validity.RowIsValid(file_idx)) {
					throw InternalException("Filename cannot be NULL!");
				}
				RecordFile(gstate, state, file_names[file_idx]);
			}
		}
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

const unordered_set<string> &IcebergCopyOnWriteRowCounter::GetFiles() const {
	if (!op_state) {
		throw InternalException("Copy-on-write DELETE: the files were read before they were recorded");
	}
	return op_state->Cast<RowCounterGlobalState>().files;
}

idx_t IcebergCopyOnWriteRowCounter::GetCount() const {
	if (!op_state) {
		throw InternalException("Copy-on-write DELETE: the rows were read before they were counted");
	}
	return op_state->Cast<RowCounterGlobalState>().count.load();
}

string IcebergCopyOnWriteRowCounter::GetName() const {
	return "ICEBERG_COPY_ON_WRITE_ROW_COUNT";
}

//===--------------------------------------------------------------------===//
// Lineage check
//===--------------------------------------------------------------------===//
IcebergCopyOnWriteLineageCheck::IcebergCopyOnWriteLineageCheck(PhysicalPlan &physical_plan, PhysicalOperator &child,
                                                               string statement)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.GetTypes(), child.estimated_cardinality),
      statement(std::move(statement)) {
	children.push_back(child);
}

OperatorResultType IcebergCopyOnWriteLineageCheck::Execute(ExecutionContext &context, DataChunk &input,
                                                           DataChunk &chunk, GlobalOperatorState &gstate,
                                                           OperatorState &state) const {
	//! The kept rows end with _row_id and _last_updated_sequence_number
	auto column_count = input.ColumnCount();
	D_ASSERT(column_count >= 2);
	if (input.size() && (VectorOperations::HasNull(input.data[column_count - 2]) ||
	                     VectorOperations::HasNull(input.data[column_count - 1]))) {
		throw InvalidInputException("Copy-on-write %s cannot preserve uncommitted row lineage. "
		                            "Commit the earlier changes to this table before running the %s.",
		                            statement, statement);
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

string IcebergCopyOnWriteLineageCheck::GetName() const {
	return "ICEBERG_COPY_ON_WRITE_LINEAGE_CHECK";
}

//===--------------------------------------------------------------------===//
// MERGE positions
//===--------------------------------------------------------------------===//
IcebergCopyOnWriteMergePositions::IcebergCopyOnWriteMergePositions(PhysicalPlan &physical_plan,
                                                                   PhysicalOperator &merge_plan,
                                                                   shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows,
                                                                   shared_ptr<IcebergCopyOnWriteMergeResult> result)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::VARCHAR, LogicalType::BIGINT},
                       merge_plan.estimated_cardinality),
      merge_rows(std::move(merge_rows)), result(std::move(result)) {
	children.push_back(merge_plan);
}

SinkResultType IcebergCopyOnWriteMergePositions::Sink(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSinkInput &input) const {
	for (idx_t i = 0; i < chunk.size(); i++) {
		result->merged_count += NumericCast<idx_t>(chunk.GetValue(0, i).GetValue<int64_t>());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SourceResultType IcebergCopyOnWriteMergePositions::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                   OperatorSourceInput &input) const {
	lock_guard<mutex> guard(merge_rows->lock);
	auto &rows_by_file = merge_rows->deletes.rows_by_file;
	while (!rows_by_file.empty() && rows_by_file.begin()->second.empty()) {
		rows_by_file.erase(rows_by_file.begin());
	}
	if (rows_by_file.empty()) {
		return SourceResultType::FINISHED;
	}
	auto &entry = *rows_by_file.begin();
	auto &positions = entry.second;
	auto count = MinValue<idx_t>(positions.size(), STANDARD_VECTOR_SIZE);
	chunk.data[0].SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<string_t>(chunk.data[0])[0] = StringVector::AddString(chunk.data[0], entry.first);
	auto row_numbers = FlatVector::GetDataMutable<int64_t>(chunk.data[1]);
	auto start = positions.size() - count;
	for (idx_t i = 0; i < count; i++) {
		row_numbers[i] = NumericCast<int64_t>(positions[start + i]);
	}
	positions.resize(start);
	if (positions.empty()) {
		rows_by_file.erase(rows_by_file.begin());
	} else {
		positions.shrink_to_fit();
	}
	result->removed_positions += count;
	chunk.SetChildCardinality(count);
	return rows_by_file.empty() ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

string IcebergCopyOnWriteMergePositions::GetName() const {
	return "ICEBERG_COPY_ON_WRITE_MERGE_POSITIONS";
}

//===--------------------------------------------------------------------===//
// Rewrite
//===--------------------------------------------------------------------===//
namespace {

//! SPEC: when removing a data file, writers must also remove any deletion vector that applies to it. Like a
//! merge-on-read DELETE replacing a vector, this also removes the file-scoped position delete files; partition-scoped
//! ones may still apply to other data files and stay.
void AddReplacedFile(IcebergFileRewriteInput &result, const string &file, const IcebergMultiFileList &target_files) {
	result.replaced_files.emplace_back(file);
	auto existing_deletes = target_files.GetExistingPositionalDeleteData(file);
	if (existing_deletes) {
		for (auto &delete_file : existing_deletes->source_files) {
			result.replaced_files.push_back(delete_file);
		}
	}
}

} // namespace

IcebergCopyOnWriteRewrite::IcebergCopyOnWriteRewrite(PhysicalPlan &physical_plan, IcebergTable &table,
                                                     IcebergCopyOnWriteRowCounter &touched_rows,
                                                     optional_ptr<IcebergCopyOnWriteRowCounter> updated_rows,
                                                     shared_ptr<IcebergCopyOnWriteMergeResult> merge_result,
                                                     const IcebergMultiFileList &target_files,
                                                     unique_ptr<LogicalOperator> logical_plan)
    : IcebergFileRewrite(physical_plan, {LogicalType::BIGINT}, 1, IcebergWriteOperation::COPY_ON_WRITE_DELETE),
      table(table), touched_rows(touched_rows), updated_rows(updated_rows), merge_result(std::move(merge_result)),
      target_files(target_files), logical_plan(std::move(logical_plan)) {
}

unique_ptr<GlobalSinkState> IcebergCopyOnWriteRewrite::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergFileRewriteGlobalState>(context, table);
}

SourceResultType IcebergCopyOnWriteRewrite::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                            OperatorSourceInput &input) const {
	auto &state = sink_state->Cast<IcebergFileRewriteGlobalState>();
	//! An UPDATE returns the rows it updated, a DELETE the rows it removed, a MERGE what the MERGE reported
	auto count = merge_result   ? merge_result->merged_count.load()
	             : updated_rows ? updated_rows->GetCount()
	                            : GetDeletedCount(state.insert_count.load());
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(count)));
	chunk.SetChildCardinality(1);
	return SourceResultType::FINISHED;
}

IcebergFileRewriteInput IcebergCopyOnWriteRewrite::GetRewriteInput(const IcebergFileRewriteGlobalState &state) const {
	//! Replacement rows come from this scan, so retain OVERWRITE's non-retryable policy.
	//! Without replacement files, DELETE only invalidates stable paths. Its manifest identities
	//! are rebuilt for each commit attempt, including those of files inserted in this transaction.
	IcebergFileRewriteInput result(state.added_data_files ? IcebergSnapshotOperationType::OVERWRITE
	                                                      : IcebergSnapshotOperationType::DELETE);
	//! Every live row of a touched file is written once, kept or updated, unless an UPDATE ... FROM matched a row
	//! more than once: then that row is written once per match.
	if (updated_rows && state.insert_count.load() != touched_rows.GetCount()) {
		throw NotImplementedException(
		    "The same row was updated multiple times - this is not (yet) supported in Iceberg. Eliminate "
		    "duplicate matches prior to running the UPDATE");
	}
	//! A MERGE writes its new rows itself, so this rewrite writes only the kept rows: touched - kept is the number
	//! of distinct positions removed. More positions than that means its actions matched a row more than once.
	if (merge_result && merge_result->removed_positions.load() != GetDeletedCount(state.insert_count.load())) {
		throw NotImplementedException(
		    "The same row was updated multiple times - this is not (yet) supported in Iceberg. Eliminate "
		    "duplicate matches prior to running the MERGE");
	}
	for (auto &file : touched_rows.GetFiles()) {
		AddReplacedFile(result, file, target_files);
	}
	return result;
}

void IcebergCopyOnWriteRewrite::StageRewrite(IcebergTransactionData &transaction_data,
                                             const IcebergFileRewriteGlobalState &state,
                                             IcebergSnapshotOperationType operation,
                                             vector<IcebergManifestEntry> &&new_entries,
                                             IcebergManifestDeletes &&replaced_files) const {
	if (!merge_result) {
		IcebergFileRewrite::StageRewrite(transaction_data, state, operation, std::move(new_entries),
		                                 std::move(replaced_files));
		return;
	}
	//! A MERGE's rewrite joins the snapshot its actions stage
	if (operation == IcebergSnapshotOperationType::DELETE) {
		transaction_data.AddDeleteSnapshot(partitioned_manifest_entry_map_t(), std::move(replaced_files), true);
	} else {
		transaction_data.AddSnapshot(operation, std::move(new_entries), std::move(replaced_files), true);
	}
}

idx_t IcebergCopyOnWriteRewrite::GetDeletedCount(idx_t kept) const {
	if (touched_rows.GetFiles().empty()) {
		return 0;
	}
	auto touched = touched_rows.GetCount();
	if (kept > touched) {
		throw InternalException("Copy-on-write DELETE: kept %llu rows of the %llu rows of the touched files", kept,
		                        touched);
	}
	return touched - kept;
}

} // namespace duckdb
