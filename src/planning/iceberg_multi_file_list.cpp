#include "planning/iceberg_multi_file_list.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"

#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

IcebergMultiFileList::IcebergMultiFileList(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info,
                                           const string &path, const IcebergOptions &options)
    : planner(make_uniq<IcebergScanPlanner>(context, std::move(scan_info), path, options)),
      delete_execution(make_shared_ptr<IcebergDeleteExecutionState>()) {
}

IcebergMultiFileList::IcebergMultiFileList(unique_ptr<IcebergScanPlanner> planner_p,
                                           shared_ptr<IcebergDeleteExecutionState> delete_execution_p)
    : planner(std::move(planner_p)), delete_execution(std::move(delete_execution_p)) {
}

IcebergMultiFileList::~IcebergMultiFileList() {
}

IcebergScanPlanner &IcebergMultiFileList::GetScanPlanner() {
	return *planner;
}

const IcebergScanPlanner &IcebergMultiFileList::GetScanPlanner() const {
	return *planner;
}

void IcebergMultiFileList::SetTable(IcebergTableSchemaVersion &table) {
	planner->SetTable(table);
}

optional_ptr<IcebergTableSchemaVersion> IcebergMultiFileList::GetTable() const {
	return planner->GetTable();
}

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (have_bound) {
		names = StringsToIdentifiers(this->names);
		return_types = types;
		return;
	}
	if (!planner->HasScanInfo()) {
		D_ASSERT(!planner->GetPath().empty());
		auto resolved_metadata =
		    IcebergUtils::ResolveTableMetadata(planner->GetContext(), planner->GetPath(), planner->GetOptions());
		auto temp_data = make_uniq<IcebergScanTemporaryData>();
		temp_data->metadata = std::move(resolved_metadata.metadata);
		auto &metadata = temp_data->metadata;
		auto snapshot_info = metadata.GetSnapshot(*planner->GetOptions().snapshot_lookup);
		auto schema = metadata.GetSchemaFromId(snapshot_info.schema_id);
		planner->SetScanInfo(make_shared_ptr<IcebergScanInfo>(resolved_metadata.table_location, std::move(temp_data),
		                                                      snapshot_info, *schema));
	}
	for (auto &schema_entry : planner->GetSchema().columns) {
		names.push_back(Identifier(schema_entry->name));
		return_types.push_back(schema_entry->type);
	}
	QueryResult::DeduplicateColumns(names);
	for (idx_t i = 0; i < names.size(); i++) {
		planner->GetSchema().columns[i]->name = names[i].GetIdentifierName();
	}
	have_bound = true;
	this->names = IdentifiersToStrings(names);
	types = return_types;
}

shared_ptr<IcebergDeleteData> IcebergMultiFileList::GetExistingPositionalDeleteData(const string &file_path) const {
	return delete_execution->GetExistingPositionalDeleteData(file_path);
}

IcebergDeletePlan IcebergMultiFileList::ProcessDeletes(const IcebergScanTask &task) const {
	return delete_execution->ProcessDeletes(*planner, task);
}

unique_ptr<IcebergMultiFileList>
IcebergMultiFileList::PushdownInternal(TableFilterSet &new_filters, const vector<ColumnIndex> &column_indexes) const {
	IcebergTableFilters result_filter_set;
	for (auto &entry : new_filters) {
		auto projection_index = ProjectionIndex(entry.GetIndex().GetIndex());
		auto &column_index = column_indexes[projection_index];
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= names.size()) {
			continue;
		}
		auto &filter = ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::PushdownInternal");
		result_filter_set.PushFilter(column_index, filter.Copy());
	}
	auto result = unique_ptr<IcebergMultiFileList>(
	    new IcebergMultiFileList(planner->CreateView(std::move(result_filter_set)), delete_execution));
	result->have_bound = true;
	result->names = names;
	result->types = types;
	return result;
}

unique_ptr<MultiFileList>
IcebergMultiFileList::DynamicFilterPushdown(MultiFileDynamicPushdownInfo &pushdown_info) const {
	auto &column_indexes = pushdown_info.column_indexes;
	auto &filters = pushdown_info.filters;
	if (!filters.HasFilters()) {
		return nullptr;
	}

	auto filters_copy = filters.Copy();
	D_ASSERT(filters_copy->FilterCount() >= planner->Filters().FilterCount());
	bool filters_changed = false;
	for (auto &entry : filters) {
		auto &filter =
		    ExpressionFilter::GetExpressionFilter(entry.Filter(), "IcebergMultiFileList::DynamicFilterPushdown");
		auto column_id = column_indexes[entry.GetIndex().GetIndex()];
		auto previous = planner->Filters().TryGetFilterByColumnIndex(column_id);
		if (!previous || !filter.Equals(*previous)) {
			filters_changed = true;
		}
	}
	return filters_changed ? PushdownInternal(*filters_copy, column_indexes) : nullptr;
}

unique_ptr<MultiFileList> IcebergMultiFileList::ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) const {
	if (filters.empty()) {
		return nullptr;
	}
	FilterCombiner combiner(context);
	for (const auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}
	vector<FilterPushdownResult> unused;
	auto filter_set = combiner.GenerateTableScanFilters(info.column_indexes, unused);
	if (!filter_set.HasFilters()) {
		return nullptr;
	}
	return PushdownInternal(filter_set, info.column_indexes);
}

OpenFileInfo IcebergMultiFileList::GetFileInternal(idx_t file_id) const {
	auto task = planner->GetDataFileTask(file_id);
	if (!task) {
		return OpenFileInfo();
	}
	auto &manifest_entry = task->manifest_entry;
	auto &data_file = manifest_entry.entry.data_file;
	if (!StringUtil::CIEquals(data_file.file_format, "parquet")) {
		throw NotImplementedException("File format '%s' not supported, only supports 'parquet' currently",
		                              data_file.file_format);
	}
	OpenFileInfo result(task->file_path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	extended_info->options["file_size"] = Value::UBIGINT(data_file.file_size_in_bytes);
	extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	extended_info->options["etag"] = Value("");
	extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	if (task->manifest_entry.HasFirstRowId()) {
		extended_info->options["first_row_id"] = Value::BIGINT(task->manifest_entry.GetFirstRowId());
	}

	sequence_number_t sequence_number;
	planner->WithManifestFile(task->manifest_entry, IcebergManifestContentType::DATA,
	                          [&sequence_number, &manifest_entry](const IcebergManifestFile &manifest) {
		                          sequence_number = manifest_entry.entry.GetSequenceNumber(manifest);
	                          });

	extended_info->options["sequence_number"] = Value::BIGINT(sequence_number);
	result.extended_info = std::move(extended_info);
	return result;
}

vector<OpenFileInfo> IcebergMultiFileList::GetAllFiles() const {
	vector<OpenFileInfo> result;
	for (idx_t i = 0;; i++) {
		auto file = GetFileInternal(i);
		if (file.path.empty()) {
			break;
		}
		result.push_back(std::move(file));
	}
	return result;
}

FileExpandResult IcebergMultiFileList::GetExpandResult() const {
	if (have_bound) {
		GetFileInternal(1);
	}
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t IcebergMultiFileList::GetTotalFileCount() const {
	return planner->GetTotalFileCount();
}

unique_ptr<NodeStatistics> IcebergMultiFileList::GetCardinality(ClientContext &) const {
	return planner->GetCardinality();
}

OpenFileInfo IcebergMultiFileList::GetFile(idx_t file_id) const {
	return GetFileInternal(file_id);
}

} // namespace duckdb
