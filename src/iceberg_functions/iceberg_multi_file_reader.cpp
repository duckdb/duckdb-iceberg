#include "iceberg_multi_file_reader.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "iceberg_predicate.hpp"
#include "iceberg_value.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

#include "metadata/iceberg_predicate_stats.hpp"
#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {

IcebergMultiFileReader::IcebergMultiFileReader(shared_ptr<TableFunctionInfo> function_info)
    : function_info(function_info) {
}

unique_ptr<MultiFileReader> IcebergMultiFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<IcebergMultiFileReader>(table.function_info);
}

shared_ptr<MultiFileList> IcebergMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                 const FileGlobInput &glob_input) {
	if (paths.size() != 1) {
		throw BinderException("'iceberg_scan' only supports single path as input");
	}

	//! Scan initiated from a REST Catalog
	auto scan_info = shared_ptr_cast<TableFunctionInfo, IcebergScanInfo>(function_info);
	return make_shared_ptr<IcebergMultiFileList>(context, scan_info, paths[0], options);
}

static MultiFileColumnDefinition TransformColumn(const IcebergColumnDefinition &input) {
	MultiFileColumnDefinition column(input.name, input.type);
	if (input.initial_default.IsNull()) {
		column.default_expression = make_uniq<ConstantExpression>(Value(input.type));
	} else {
		column.default_expression = make_uniq<ConstantExpression>(input.initial_default);
	}
	column.identifier = Value::INTEGER(input.id);
	for (auto &child : input.children) {
		column.children.push_back(TransformColumn(*child));
	}
	return column;
}

bool IcebergMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                  vector<string> &names, MultiFileReaderBindData &bind_data) {
	auto &iceberg_multi_file_list = dynamic_cast<IcebergMultiFileList &>(files);

	iceberg_multi_file_list.Bind(return_types, names);
	// FIXME: apply final transformation for 'file_row_number' ???

	auto &schema = iceberg_multi_file_list.GetSchema().columns;
	auto &columns = bind_data.schema;
	for (auto &item : schema) {
		columns.push_back(TransformColumn(*item));
	}
	bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
	return true;
}

void IcebergMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                         vector<LogicalType> &return_types, vector<string> &names,
                                         MultiFileReaderBindData &bind_data) {
	// Disable all other multifilereader options
	options.auto_detect_hive_partitioning = false;
	options.hive_partitioning = false;
	options.union_by_name = false;

	MultiFileReader::BindOptions(options, files, return_types, names, bind_data);
}

unique_ptr<MultiFileReaderGlobalState>
IcebergMultiFileReader::InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
                                              const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                                              const vector<MultiFileColumnDefinition> &global_columns,
                                              const vector<ColumnIndex> &global_column_ids) {
	vector<LogicalType> extra_columns;
	auto res = make_uniq<IcebergMultiFileReaderGlobalState>(extra_columns, file_list);
	return std::move(res);
}

static void ApplyFieldMapping(MultiFileColumnDefinition &col, const vector<IcebergFieldMapping> &mappings,
                              const case_insensitive_map_t<idx_t> &fields,
                              optional_ptr<MultiFileColumnDefinition> parent = nullptr) {
	if (!col.identifier.IsNull()) {
		return;
	}

	auto name = col.name;
	if (parent && parent->type.id() == LogicalTypeId::MAP && StringUtil::CIEquals(name, "key_value")) {
		//! Deal with MAP, it has a 'key_value' child, which holds the 'key' + 'value' columns
		for (auto &child : col.children) {
			ApplyFieldMapping(child, mappings, fields, parent);
		}
		return;
	}
	if (parent && parent->type.id() == LogicalTypeId::LIST && StringUtil::CIEquals(name, "list")) {
		//! Deal with LIST, it has a 'element' child, which has the column for the underlying list data
		name = "element";
	}

	auto it = fields.find(name);
	if (it == fields.end()) {
		throw InvalidConfigurationException("Column '%s' does not have a field-id, and no field-mapping exists for it!",
		                                    name);
	}
	auto &mapping = mappings[it->second];

	if (mapping.field_id != NumericLimits<int32_t>::Maximum()) {
		col.identifier = Value::INTEGER(mapping.field_id);
	}

	for (auto &child : col.children) {
		ApplyFieldMapping(child, mappings, mapping.field_mapping_indexes, col);
	}
}

static Value TransformPartitionValueFromBlob(const string_t &blob, const LogicalType &type) {
	auto result = IcebergValue::DeserializeValue(blob, type);
	if (result.HasError()) {
		throw InvalidConfigurationException(result.GetError());
	}
	return result.GetValue();
}

template <class T>
static Value TransformPartitionValueTemplated(const Value &value, const LogicalType &type) {
	T val = value.GetValue<T>();
	string_t blob((const char *)&val, sizeof(T));
	return TransformPartitionValueFromBlob(blob, type);
}

static Value TransformPartitionValue(const Value &value, const LogicalType &type) {
	D_ASSERT(!value.type().IsNested());
	switch (value.type().InternalType()) {
	case PhysicalType::BOOL:
		return TransformPartitionValueTemplated<bool>(value, type);
	case PhysicalType::INT8:
		return TransformPartitionValueTemplated<int8_t>(value, type);
	case PhysicalType::INT16:
		return TransformPartitionValueTemplated<int16_t>(value, type);
	case PhysicalType::INT32:
		return TransformPartitionValueTemplated<int32_t>(value, type);
	case PhysicalType::INT64:
		return TransformPartitionValueTemplated<int64_t>(value, type);
	case PhysicalType::INT128:
		return TransformPartitionValueTemplated<hugeint_t>(value, type);
	case PhysicalType::FLOAT:
		return TransformPartitionValueTemplated<float>(value, type);
	case PhysicalType::DOUBLE:
		return TransformPartitionValueTemplated<double>(value, type);
	case PhysicalType::VARCHAR: {
		return TransformPartitionValueFromBlob(value.GetValueUnsafe<string_t>(), type);
	}
	default:
		throw NotImplementedException("TransformPartitionValue: Value: '%s' -> '%s'", value.ToString(),
		                              type.ToString());
	}
}

static void ApplyPartitionConstants(const IcebergMultiFileList &multi_file_list, MultiFileReaderData &reader_data,
                                    const vector<MultiFileColumnDefinition> &global_columns,
                                    const vector<ColumnIndex> &global_column_ids) {
	// Get the metadata for this file
	auto &reader = *reader_data.reader;
	auto file_id = reader.file_list_idx.GetIndex();
	auto &manifest_entry = multi_file_list.manifest_entries[file_id];
	auto &data_file = manifest_entry.data_file;

	// Get the partition spec for this file
	auto &partition_specs = multi_file_list.GetMetadata().partition_specs;
	auto spec_id = manifest_entry.partition_spec_id;
	auto partition_spec_it = partition_specs.find(spec_id);
	if (partition_spec_it == partition_specs.end()) {
		throw InvalidConfigurationException("'partition_spec_id' %d doesn't exist in the metadata", spec_id);
	}

	auto &partition_spec = partition_spec_it->second;
	if (partition_spec.fields.empty()) {
		return; // No partition fields, continue with normal mapping
	}

	unordered_map<uint64_t, idx_t> identifier_to_field_index;
	for (idx_t i = 0; i < partition_spec.fields.size(); i++) {
		auto &field = partition_spec.fields[i];
		identifier_to_field_index[field.source_id] = i;
	}

	auto &local_columns = reader.columns;
	unordered_map<uint64_t, idx_t> local_field_id_to_index;
	for (idx_t i = 0; i < local_columns.size(); i++) {
		auto &local_column = local_columns[i];
		auto field_identifier = local_column.identifier.GetValue<int32_t>();
		auto field_id = static_cast<uint64_t>(field_identifier);
		local_field_id_to_index[field_id] = i;
	}

	for (idx_t i = 0; i < global_column_ids.size(); i++) {
		auto global_id = global_column_ids[i];
		if (global_id.IsVirtualColumn()) {
			continue;
		}
		auto &global_column = global_columns[global_id.GetPrimaryIndex()];
		auto field_id = static_cast<uint64_t>(global_column.identifier.GetValue<int32_t>());
		if (local_field_id_to_index.count(field_id)) {
			//! Column exists in the local columns of the file
			continue;
		}

		auto it = identifier_to_field_index.find(field_id);
		if (it == identifier_to_field_index.end()) {
			continue;
		}

		auto &field = partition_spec.fields[it->second];
		if (field.transform != IcebergTransformType::IDENTITY) {
			continue; // Skip non-identity transforms
		}

		// Get the partition value from the data file's partition struct
		auto &partition_values = data_file.partition_values;
		if (partition_values.empty()) {
			continue; // No partition value available
		}
		optional_ptr<const Value> partition_value;
		for (auto &it : partition_values) {
			if (static_cast<uint64_t>(it.first) == field.partition_field_id && !it.second.IsNull()) {
				partition_value = it.second;
				break;
			}
		}
		if (!partition_value) {
			//! This data file doesn't have a value for this partition field (is that an error ??)
			continue;
		}
		auto global_idx = MultiFileGlobalIndex(i);
		reader_data.constant_map.Add(global_idx, TransformPartitionValue(*partition_value, global_column.type));
	}
}

void IcebergMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                          const MultiFileReaderBindData &options,
                                          const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                          optional_ptr<MultiFileReaderGlobalState> global_state) {
	MultiFileReader::FinalizeBind(reader_data, file_options, options, global_columns, global_column_ids, context,
	                              global_state);
	D_ASSERT(global_state);
	// Get the metadata for this file
	const auto &multi_file_list = dynamic_cast<const IcebergMultiFileList &>(*global_state->file_list);
	auto &reader = *reader_data.reader;
	auto file_id = reader.file_list_idx.GetIndex();

	{
		lock_guard<mutex> guard(multi_file_list.lock);
		const auto &manifest_entry = multi_file_list.manifest_entries[file_id];
		const auto &data_file = manifest_entry.data_file;
		// The path of the data file where this chunk was read from
		const auto &file_path = data_file.file_path;
		lock_guard<mutex> delete_guard(multi_file_list.delete_lock);
		if (multi_file_list.current_delete_manifest != multi_file_list.delete_manifests.end() ||
		    multi_file_list.transaction_delete_idx < multi_file_list.transaction_delete_manifests.size()) {
			multi_file_list.ProcessDeletes(global_columns, global_column_ids);
		}
		reader.deletion_filter = std::move(multi_file_list.GetPositionalDeletesForFile(file_path));
	}

	auto &local_columns = reader_data.reader->columns;
	auto &metadata = multi_file_list.GetMetadata();
	auto &mappings = metadata.mappings;
	if (!multi_file_list.GetMetadata().mappings.empty()) {
		auto &root = metadata.mappings[0];
		for (auto &local_column : local_columns) {
			ApplyFieldMapping(local_column, mappings, root.field_mapping_indexes);
		}
	}
	ApplyPartitionConstants(multi_file_list, reader_data, global_columns, global_column_ids);
}

void IcebergMultiFileReader::ApplyEqualityDeletes(ClientContext &context, DataChunk &output_chunk,
                                                  const IcebergMultiFileList &multi_file_list,
                                                  const IcebergManifestEntry &manifest_entry,
                                                  const vector<MultiFileColumnDefinition> &local_columns) {
	vector<reference<IcebergEqualityDeleteRow>> delete_rows;

	auto &data_file = manifest_entry.data_file;
	auto &metadata = multi_file_list.GetMetadata();
	auto delete_data_it = multi_file_list.equality_delete_data.upper_bound(manifest_entry.sequence_number);
	//! Look through all the equality delete files with a *higher* sequence number
	for (; delete_data_it != multi_file_list.equality_delete_data.end(); delete_data_it++) {
		auto &files = delete_data_it->second->files;
		for (auto &file : files) {
			auto &partition_spec = metadata.partition_specs.at(file.partition_spec_id);
			if (partition_spec.IsPartitioned()) {
				if (file.partition_spec_id != manifest_entry.partition_spec_id) {
					//! Not unpartitioned and the data does not share the same partition spec as the delete, skip the
					//! delete file.
					continue;
				}
				D_ASSERT(file.partition_values.size() == data_file.partition_values.size());
				for (idx_t i = 0; i < file.partition_values.size(); i++) {
					if (file.partition_values[i] != data_file.partition_values[i]) {
						//! Same partition spec id, but the partitioning information doesn't match, delete file doesn't
						//! apply.
						continue;
					}
				}
			}
			delete_rows.insert(delete_rows.end(), file.rows.begin(), file.rows.end());
		}
	}

	if (delete_rows.empty()) {
		return;
	}

	//! Map from column_id to 'local_columns' index
	unordered_map<int32_t, column_t> id_to_local_column;
	for (column_t i = 0; i < local_columns.size(); i++) {
		auto &col = local_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_local_column[col.identifier.GetValue<int32_t>()] = i;
	}

	//! Create a big CONJUNCTION_AND of all the rows, illustrative example:
	//! WHERE
	//!	(col1 != 'A' OR col2 != 'B') AND
	//!	(col1 != 'C' OR col2 != 'D') AND
	//!	(col1 != 'X' OR col2 != 'Y') AND
	//!	(col1 != 'Z' OR col2 != 'W')

	vector<unique_ptr<Expression>> rows;
	for (auto &row : delete_rows) {
		vector<unique_ptr<Expression>> equalities;
		for (auto &item : row.get().filters) {
			auto &field_id = item.first;
			auto &expression = item.second;

			bool treat_as_null = !id_to_local_column.count(field_id);
			if (treat_as_null) {
				//! This column is not present in the file
				//! For the purpose of the equality deletes, we are treating it as if its value is NULL (despite any
				//! 'initial-default' that exists)

				//! This means that if the expression is 'IS_NOT_NULL', the result is False for this column, otherwise
				//! it's True (because nothing compares equal to NULL)
				if (expression->type == ExpressionType::OPERATOR_IS_NOT_NULL) {
					equalities.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(false)));
				} else {
					equalities.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
				}
			} else {
				equalities.push_back(expression->Copy());
			}
		}

		unique_ptr<Expression> filter;
		D_ASSERT(!equalities.empty());
		if (equalities.size() > 1) {
			auto conjunction_or = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
			conjunction_or->children = std::move(equalities);
			filter = std::move(conjunction_or);
		} else {
			filter = std::move(equalities[0]);
		}
		rows.push_back(std::move(filter));
	}

	unique_ptr<Expression> equality_delete_filter;
	D_ASSERT(!rows.empty());
	if (rows.size() == 1) {
		equality_delete_filter = std::move(rows[0]);
	} else {
		auto conjunction_and = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		conjunction_and->children = std::move(rows);
		equality_delete_filter = std::move(conjunction_and);
	}

	//! Apply equality deletes
	ExpressionExecutor expression_executor(context);
	expression_executor.AddExpression(*equality_delete_filter);
	SelectionVector sel_vec(STANDARD_VECTOR_SIZE);
	idx_t count = expression_executor.SelectExpression(output_chunk, sel_vec);
	output_chunk.Slice(sel_vec, count);
}

void IcebergMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data,
                                           BaseFileReader &reader, const MultiFileReaderData &reader_data,
                                           DataChunk &input_chunk, DataChunk &output_chunk,
                                           ExpressionExecutor &executor,
                                           optional_ptr<MultiFileReaderGlobalState> global_state) {
	// Base class finalization first
	MultiFileReader::FinalizeChunk(context, bind_data, reader, reader_data, input_chunk, output_chunk, executor,
	                               global_state);

	D_ASSERT(global_state);
	// Get the metadata for this file
	const auto &multi_file_list = dynamic_cast<const IcebergMultiFileList &>(*global_state->file_list);
	auto file_id = reader.file_list_idx.GetIndex();
	auto &manifest_entry = multi_file_list.manifest_entries[file_id];
	auto &local_columns = reader.columns;
	ApplyEqualityDeletes(context, output_chunk, multi_file_list, manifest_entry, local_columns);
}

bool IcebergMultiFileReader::ParseOption(const string &key, const Value &val, MultiFileOptions &options,
                                         ClientContext &context) {
	auto loption = StringUtil::Lower(key);
	auto &snapshot_lookup = this->options.snapshot_lookup;

	if (loption == "allow_moved_paths") {
		this->options.allow_moved_paths = BooleanValue::Get(val);
		return true;
	}
	if (loption == "metadata_compression_codec") {
		this->options.metadata_compression_codec = StringValue::Get(val);
		return true;
	}
	if (loption == "version") {
		this->options.table_version = StringValue::Get(val);
		return true;
	}
	if (loption == "version_name_format") {
		auto value = StringValue::Get(val);
		auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
		if (string_substitutions != 2) {
			throw InvalidInputException("'version_name_format' has to contain two occurrences of '%%s' in it, found %d",
			                            string_substitutions);
		}
		this->options.version_name_format = value;
		return true;
	}
	if (loption == "snapshot_from_id") {
		if (snapshot_lookup.snapshot_source != SnapshotSource::LATEST) {
			throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
		}
		snapshot_lookup.snapshot_source = SnapshotSource::FROM_ID;
		snapshot_lookup.snapshot_id = val.GetValue<uint64_t>();
		return true;
	}
	if (loption == "snapshot_from_timestamp") {
		if (snapshot_lookup.snapshot_source != SnapshotSource::LATEST) {
			throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
		}
		snapshot_lookup.snapshot_source = SnapshotSource::FROM_TIMESTAMP;
		snapshot_lookup.snapshot_timestamp = val.GetValue<timestamp_t>();
		return true;
	}
	return MultiFileReader::ParseOption(key, val, options, context);
}

unique_ptr<Expression> IcebergMultiFileReader::GetVirtualColumnExpression(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &local_columns,
    idx_t &column_id, const LogicalType &type, MultiFileLocalIndex local_idx,
    optional_ptr<MultiFileColumnDefinition> &global_column_reference) {
	return MultiFileReader::GetVirtualColumnExpression(context, reader_data, local_columns, column_id, type, local_idx,
	                                                   global_column_reference);
}

} // namespace duckdb
