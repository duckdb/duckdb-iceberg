#include "planning/iceberg_multi_file_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/logging/logger.hpp"
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

#include "common/iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/pruning/iceberg_predicate.hpp"
#include "core/expression/iceberg_value.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"

namespace duckdb {

using MultiFileColumnPath = vector<idx_t>;

static void PopulateFieldIdMap(const vector<MultiFileColumnDefinition> &columns,
                               unordered_map<int32_t, MultiFileColumnPath> &field_id_map,
                               MultiFileColumnPath &column_path) {
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &column = columns[i];
		column_path.push_back(i);
		if (!column.identifier.IsNull()) {
			field_id_map[column.GetIdentifierFieldId()] = column_path;
		}
		PopulateFieldIdMap(column.children, field_id_map, column_path);
		column_path.pop_back();
	}
}

static unordered_map<int32_t, MultiFileColumnPath> CreateFieldIdMap(const vector<MultiFileColumnDefinition> &columns) {
	unordered_map<int32_t, MultiFileColumnPath> result;
	MultiFileColumnPath column_path;
	PopulateFieldIdMap(columns, result, column_path);
	return result;
}

static const MultiFileColumnDefinition &GetColumnFromPath(const vector<MultiFileColumnDefinition> &columns,
                                                          const MultiFileColumnPath &column_path) {
	D_ASSERT(!column_path.empty());
	reference<const vector<MultiFileColumnDefinition>> current_columns(columns);
	optional_ptr<const MultiFileColumnDefinition> result;
	for (auto column_index : column_path) {
		D_ASSERT(column_index < current_columns.get().size());
		result = current_columns.get()[column_index];
		current_columns = result->children;
	}
	return *result;
}

static ColumnIndex CreateColumnIndex(const MultiFileColumnPath &column_path) {
	D_ASSERT(!column_path.empty());
	ColumnIndex result(column_path.back());
	for (idx_t depth = column_path.size() - 1; depth > 0; depth--) {
		vector<ColumnIndex> children;
		children.push_back(std::move(result));
		result = ColumnIndex(column_path[depth - 1], std::move(children));
	}
	return result;
}

IcebergMultiFileReader::IcebergMultiFileReader(shared_ptr<TableFunctionInfo> function_info)
    : function_info(function_info) {
	row_id_column = make_uniq<MultiFileColumnDefinition>("_row_id", LogicalType::BIGINT);
	row_id_column->identifier = Value::INTEGER(MultiFileReader::ROW_ID_FIELD_ID);
	last_updated_sequence_number_column =
	    make_uniq<MultiFileColumnDefinition>("_last_updated_sequence_number", LogicalType::BIGINT);
	last_updated_sequence_number_column->identifier = Value::INTEGER(MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID);
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

bool IcebergMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                  vector<Identifier> &names, MultiFileReaderBindData &bind_data) {
	auto &iceberg_multi_file_list = dynamic_cast<IcebergMultiFileList &>(files);

	iceberg_multi_file_list.SetOptions(this->options);
	iceberg_multi_file_list.Bind(return_types, names);
	// FIXME: apply final transformation for 'file_row_number' ???
	auto &schema = iceberg_multi_file_list.GetSchema().columns;
	auto &columns = bind_data.schema;
	for (auto &item : schema) {
		columns.push_back(item->GetMultiFileColumnDefinition());
	}

	bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
	return true;
}

void IcebergMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                         vector<LogicalType> &return_types, vector<Identifier> &names,
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
	return make_uniq<IcebergMultiFileReaderGlobalState>(file_list);
}

IcebergEqualityDeleteReadColumn IcebergMultiFileReader::AddEqualityDeleteColumn(
    const IcebergTableMetadata &metadata, int32_t field_id, vector<MultiFileColumnDefinition> &scan_columns,
    vector<ColumnIndex> &scan_column_ids, MultiFileReaderData &reader_data, ClientContext &context) {
	auto field_id_to_scan_column = CreateFieldIdMap(scan_columns);
	MultiFileColumnPath column_path;
	auto field_entry = field_id_to_scan_column.find(field_id);
	if (field_entry == field_id_to_scan_column.end()) {
		auto column = metadata.FindColumnByFieldId(field_id);
		if (!column) {
			throw InvalidConfigurationException(
			    "Column %d must be read to apply equality deletes, but no schema contains that field id", field_id);
		}
		auto new_column = column->GetMultiFileColumnDefinition();
		// Equality-delete matching treats a field that is absent from a data file as NULL,
		// independently of any historical initial default.
		new_column.default_expression = make_uniq<ConstantExpression>(Value(new_column.type));
		scan_columns.push_back(std::move(new_column));
		column_path.push_back(scan_columns.size() - 1);
		DUCKDB_LOG(context, IcebergLogType, "Reading dropped column '%s' privately for equality deletes", column->name);
	} else {
		column_path = field_entry->second;
	}

	auto equality_column_id = CreateColumnIndex(column_path);
	auto root_column_index = equality_column_id.GetPrimaryIndex();
	auto &column = GetColumnFromPath(scan_columns, column_path);
	if (equality_column_id.HasChildren()) {
		equality_column_id.SetPushdownExtractType(scan_columns[root_column_index].type);
	}
	idx_t expression_index = DConstants::INVALID_INDEX;
	if (column_path.size() == 1) {
		//! A nested query projection can contain additional struct-extract expressions outside the scan.
		//! Always scan nested equality fields privately so their expression is independent of that projection.
		for (idx_t i = 0; i < scan_column_ids.size(); i++) {
			if (scan_column_ids[i] == equality_column_id) {
				expression_index = i;
				break;
			}
		}
	}
	if (expression_index == DConstants::INVALID_INDEX) {
		expression_index = scan_column_ids.size();
		scan_column_ids.push_back(std::move(equality_column_id));
		reader_data.extra_columns.push_back(column.type);
	}
	return {field_id, expression_index, column.type};
}

vector<IcebergEqualityDeleteReadColumn> IcebergMultiFileReader::AddEqualityDeleteColumns(
    const IcebergTableMetadata &metadata, const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
    vector<MultiFileColumnDefinition> &scan_columns, vector<ColumnIndex> &scan_column_ids,
    MultiFileReaderData &reader_data, ClientContext &context) {
	set<int32_t> required_field_ids;
	for (auto &delete_file_ref : delete_files) {
		for (auto field_id : delete_file_ref.get().equality_ids) {
			required_field_ids.insert(field_id);
		}
	}

	vector<IcebergEqualityDeleteReadColumn> result;
	for (auto field_id : required_field_ids) {
		result.push_back(
		    AddEqualityDeleteColumn(metadata, field_id, scan_columns, scan_column_ids, reader_data, context));
	}
	return result;
}

static void ApplyFieldMapping(MultiFileColumnDefinition &col, const vector<IcebergFieldMapping> &mappings,
                              const case_insensitive_map_t<idx_t> &fields, ClientContext &context,
                              optional_ptr<MultiFileColumnDefinition> parent = nullptr) {
	if (!col.identifier.IsNull()) {
		return;
	}

	auto name = col.name;
	if (parent && parent->type.id() == LogicalTypeId::MAP && name == "key_value") {
		//! Deal with MAP, it has a 'key_value' child, which holds the 'key' + 'value' columns
		for (auto &child : col.children) {
			ApplyFieldMapping(child, mappings, fields, context, parent);
		}
		return;
	}
	if (parent && parent->type.id() == LogicalTypeId::LIST && name == "list") {
		//! Deal with LIST, it has a 'element' child, which has the column for the underlying list data
		name = "element";
	}

	auto it = fields.find(name.GetIdentifierName());
	if (it == fields.end()) {
		DUCKDB_LOG(context, IcebergLogType, "Column '%s' does not have a field-id, and no field-mapping exists for it!",
		           name);
		return;
	}
	auto &mapping = mappings[it->second];

	if (mapping.field_id != NumericLimits<int32_t>::Maximum()) {
		col.identifier = Value::INTEGER(mapping.field_id);
	}

	for (auto &child : col.children) {
		ApplyFieldMapping(child, mappings, mapping.field_mapping_indexes, context, col);
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
	// DECIMAL partition values are already decoded as proper DuckDB DECIMALs by the Avro reader.
	// The blob round-trip below misinterprets the little-endian internal representation as
	// big-endian Iceberg bytes, producing garbage. Return directly (or cast if params differ).
	if (value.type().id() == LogicalTypeId::DECIMAL) {
		if (value.type() == type) {
			return value;
		}
		return value.DefaultCastAs(type);
	}
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

void IcebergMultiFileReader::ApplyPartitionConstants(const IcebergManifestFile &manifest_file,
                                                     const BoundIcebergManifestEntry &bound_manifest_entry,
                                                     const IcebergTableMetadata &metadata,
                                                     MultiFileReaderData &reader_data,
                                                     const vector<MultiFileColumnDefinition> &global_columns,
                                                     const vector<ColumnIndex> &global_column_ids,
                                                     ClientContext &context) {
	// Get the metadata for this file
	auto &reader = *reader_data.reader;
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &data_file = manifest_entry.data_file;

	// Get the partition spec for this file
	auto &partition_specs = metadata.partition_specs;
	auto spec_id = manifest_file.partition_spec_id;
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
		if (local_column.identifier.IsNull()) {
			continue;
		}
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

		// Get the partition value from the data file's partition info
		if (data_file.partition_info.empty()) {
			continue; // No partition info available
		}
		optional_ptr<const Value> partition_value;
		for (auto &partition_info : data_file.partition_info) {
			if (partition_info.field_id == field.partition_field_id && !partition_info.value.IsNull()) {
				partition_value = partition_info.value;
				break;
			}
		}
		if (!partition_value) {
			DUCKDB_LOG(context, IcebergLogType,
			           "Iceberg partition constant missing for data_file '%s', partition field_id=%llu column '%s'",
			           data_file.file_path, field.partition_field_id, global_column.name);
			//! This data file doesn't have a value for this partition field (is that an error ??)
			continue;
		}
		auto global_idx = MultiFileGlobalIndex(i);
		reader_data.constant_map.Add(global_idx, TransformPartitionValue(*partition_value, global_column.type));
	}
}

ReaderInitializeType IcebergMultiFileReader::InitializeReader(MultiFileReaderData &reader_data,
                                                              const MultiFileBindData &bind_data,
                                                              const vector<MultiFileColumnDefinition> &global_columns,
                                                              const vector<ColumnIndex> &global_column_ids,
                                                              optional_ptr<TableFilterSet> table_filters,
                                                              ClientContext &context, MultiFileGlobalState &gstate) {
	auto &iceberg_state = gstate.multi_file_reader_state->Cast<IcebergMultiFileReaderGlobalState>();
	const auto &multi_file_list = dynamic_cast<const IcebergMultiFileList &>(*iceberg_state.file_list);
	auto &metadata = multi_file_list.GetMetadata();
	auto file_id = reader_data.reader->file_list_idx.GetIndex();
	auto bound_manifest_entry = multi_file_list.GetManifestEntry(file_id);
	auto manifest_file = multi_file_list.GetManifestFileForDataFile(file_id);
	auto delete_plan = multi_file_list.ProcessDeletes(bound_manifest_entry);

	//! Make a copy of the global columns+column_ids, if we have equality deletes we will add columns to this
	//! This is done so CreateMapping treats these columns as required for the current file,
	//! and sets up local_column_ids+expressions for these columns.
	auto scan_columns = global_columns;
	auto scan_column_ids = global_column_ids;
	auto read_columns = AddEqualityDeleteColumns(metadata, delete_plan.equality_deletes, scan_columns, scan_column_ids,
	                                             reader_data, context);
	auto equality_delete_state = make_uniq<IcebergEqualityDeleteReadState>(std::move(read_columns));

	MultiFileReader::FinalizeBind(reader_data, bind_data.file_options, bind_data.reader_bind, scan_columns,
	                              scan_column_ids, context, gstate.multi_file_reader_state.get());

	auto &reader = *reader_data.reader;
	reader.deletion_filter = std::move(delete_plan.positional_deletes);

	auto &local_columns = reader_data.reader->columns;
	auto &mappings = metadata.mappings;
	if (!metadata.mappings.empty()) {
		auto &root = metadata.mappings[0];
		for (auto &local_column : local_columns) {
			ApplyFieldMapping(local_column, mappings, root.field_mapping_indexes, context);
		}
	}
	ApplyPartitionConstants(manifest_file, bound_manifest_entry, metadata, reader_data, scan_columns, scan_column_ids,
	                        context);

	equality_delete_state->expression =
	    CreateEqualityDeleteExpression(delete_plan.equality_deletes, local_columns, *equality_delete_state);
	iceberg_state.CacheEqualityDeleteReadState(file_id, std::move(equality_delete_state));

	return CreateMapping(context, reader_data, scan_columns, scan_column_ids, table_filters, gstate.file_list,
	                     bind_data.reader_bind, bind_data.virtual_columns);
}

void IcebergMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                          const MultiFileReaderBindData &options,
                                          const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                          optional_ptr<MultiFileReaderGlobalState> global_state) {
	throw InternalException("IcebergMultiFileReader::FinalizeBind is unreachable");
}

unique_ptr<Expression> IcebergMultiFileReader::CreateEqualityDeleteExpression(
    const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
    const vector<MultiFileColumnDefinition> &local_columns, const IcebergEqualityDeleteReadState &read_state) {
	if (delete_files.empty()) {
		return nullptr;
	}

	//! Map every field id, including nested fields, to its path in 'local_columns'.
	auto id_to_local_column = CreateFieldIdMap(local_columns);

	//! Create a big CONJUNCTION_AND of all the rows, illustrative example:
	//! WHERE
	//!	(col1 != 'A' OR col2 != 'B') AND
	//!	(col1 != 'C' OR col2 != 'D') AND
	//!	(col1 != 'X' OR col2 != 'Y') AND
	//!	(col1 != 'Z' OR col2 != 'W')

	vector<unique_ptr<Expression>> rows;
	for (auto &delete_file_ref : delete_files) {
		auto &delete_file = delete_file_ref.get();
		auto &equality_values = delete_file.equality_values;
		if (equality_values.size() == 0) {
			continue;
		}
		auto &equality_ids = delete_file.equality_ids;
		if (equality_values.ColumnCount() != equality_ids.size()) {
			throw InvalidConfigurationException("Equality delete file contains an unexpected number of columns");
		}
		auto row_count = equality_values.size();
		for (idx_t row_index = 0; row_index < row_count; row_index++) {
			vector<unique_ptr<Expression>> equalities;
			for (idx_t column_index = 0; column_index < equality_ids.size(); column_index++) {
				auto field_id = equality_ids[column_index];
				auto constant = equality_values.GetValue(column_index, row_index);

				if (!id_to_local_column.count(field_id)) {
					//! A field absent from the data file is NULL for equality-delete matching,
					//! regardless of its Iceberg initial default.
					equalities.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(!constant.IsNull())));
					continue;
				}

				auto state_entry = read_state.field_indexes.find(field_id);
				if (state_entry == read_state.field_indexes.end()) {
					throw InternalException("Missing private scan column for equality-delete field id %d", field_id);
				}
				auto equality_column_index = state_entry->second;
				auto &column = read_state.columns[equality_column_index];
				auto bound_ref = make_uniq<BoundReferenceExpression>(column.type, equality_column_index);
				if (!constant.IsNull()) {
					equalities.push_back(
					    BoundComparisonExpression::Create(ExpressionType::COMPARE_DISTINCT_FROM, std::move(bound_ref),
					                                      make_uniq<BoundConstantExpression>(constant)));
				} else {
					auto is_not_null =
					    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
					is_not_null->GetChildrenMutable().push_back(std::move(bound_ref));
					equalities.push_back(std::move(is_not_null));
				}
			}

			unique_ptr<Expression> filter;
			D_ASSERT(!equalities.empty());
			if (equalities.size() > 1) {
				auto conjunction_or = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
				conjunction_or->GetChildrenMutable() = std::move(equalities);
				filter = std::move(conjunction_or);
			} else {
				filter = std::move(equalities[0]);
			}
			rows.push_back(std::move(filter));
		}
	}
	if (rows.empty()) {
		return nullptr;
	}

	unique_ptr<Expression> equality_delete_filter;
	D_ASSERT(!rows.empty());
	if (rows.size() == 1) {
		equality_delete_filter = std::move(rows[0]);
	} else {
		auto conjunction_and = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		conjunction_and->GetChildrenMutable() = std::move(rows);
		equality_delete_filter = std::move(conjunction_and);
	}
	return equality_delete_filter;
}

void IcebergMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data,
                                           BaseFileReader &reader, const MultiFileReaderData &reader_data,
                                           DataChunk &input_chunk, DataChunk &output_chunk,
                                           ExpressionExecutor &executor,
                                           optional_ptr<MultiFileReaderGlobalState> global_state) {
	D_ASSERT(global_state);
	auto &iceberg_state = global_state->Cast<IcebergMultiFileReaderGlobalState>();

	//! Base class finalization first
	MultiFileReader::FinalizeChunk(context, bind_data, reader, reader_data, input_chunk, output_chunk, executor,
	                               global_state);

	auto file_id = reader.file_list_idx.GetIndex();
	auto &equality_delete_state = iceberg_state.GetEqualityDeleteReadState(file_id);
	auto equality_delete_expression = equality_delete_state.expression.get();
	if (equality_delete_expression) {
		ExpressionExecutor equality_delete_executor(context);
		for (auto &column : equality_delete_state.columns) {
			D_ASSERT(column.expression_index < reader_data.expressions.size());
			equality_delete_executor.AddExpression(*reader_data.expressions[column.expression_index]);
		}
		DataChunk equality_delete_chunk;
		equality_delete_chunk.Initialize(context, equality_delete_state.types);
		equality_delete_executor.Execute(input_chunk, equality_delete_chunk);

		ExpressionExecutor filter_executor(context, *equality_delete_expression);
		SelectionVector sel_vec(STANDARD_VECTOR_SIZE);
		idx_t count = filter_executor.SelectExpression(equality_delete_chunk, sel_vec);
		output_chunk.Slice(sel_vec, count);
	}
}

bool IcebergMultiFileReader::ParseOption(const Identifier &key, const Value &val, MultiFileOptions &options,
                                         ClientContext &context) {
	auto &snapshot_lookup = this->options.snapshot_lookup;

	if (key == "allow_moved_paths") {
		this->options.allow_moved_paths = BooleanValue::Get(val);
		return true;
	}
	if (key == "metadata_compression_codec") {
		this->options.metadata_compression_codec = StringValue::Get(val);
		return true;
	}
	if (key == "version") {
		this->options.table_version = StringValue::Get(val);
		return true;
	}
	if (key == "version_name_format") {
		auto value = StringValue::Get(val);
		auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
		if (string_substitutions != 2) {
			throw InvalidInputException("'version_name_format' has to contain two occurrences of '%%s' in it, found %d",
			                            string_substitutions);
		}
		this->options.version_name_format = value;
		return true;
	}
	if (key == "snapshot_from_id") {
		if (snapshot_lookup->GetSource() != SnapshotSource::LATEST) {
			throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
		}
		snapshot_lookup.emplace(IcebergSnapshotLookup::FromSnapshotId(val.GetValue<uint64_t>()));
		return true;
	}
	if (key == "snapshot_from_timestamp") {
		if (snapshot_lookup->GetSource() != SnapshotSource::LATEST) {
			throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
		}
		snapshot_lookup.emplace(IcebergSnapshotLookup::FromTimestamp(
		    val.DefaultCastAs(LogicalType::TIMESTAMP_MS).GetValue<timestamp_ms_t>()));
		return true;
	}
	return MultiFileReader::ParseOption(key, val, options, context);
}

static unique_ptr<Expression> ConstructVirtualRowIdExpression(ClientContext &context, const LogicalType &type,
                                                              const Value &val, idx_t local_idx) {
	auto row_id_expr = make_uniq<BoundConstantExpression>(val);
	auto file_row_number = make_uniq<BoundReferenceExpression>(type, local_idx);

	// generate the addition
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(row_id_expr));
	children.push_back(std::move(file_row_number));

	FunctionBinder binder(context);
	ErrorData error;
	auto function_expr =
	    binder.BindScalarFunction(Identifier::DefaultSchema(), "+", std::move(children), error, true, nullptr);
	if (error.HasError()) {
		error.Throw();
	}
	return function_expr;
}

MultiFileReaderVirtualColumnBinding IcebergMultiFileReader::GetVirtualColumnExpression(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &local_columns,
    const idx_t column_id, const LogicalType &type, MultiFileLocalIndex local_idx) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		// row id column
		// this is computed as row_id_start + file_row_number OR read from the file
		// first check if the row id is explicitly defined in this file
		// get the row id start for this file
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Extended info not found for reading row id column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("first_row_id");
		for (idx_t i = 0; i < local_columns.size(); i++) {
			auto &col = local_columns[i];
			if (col.identifier.IsNull()) {
				continue;
			}
			if (col.identifier.GetValue<int32_t>() == MultiFileReader::ROW_ID_FIELD_ID) {
				if (entry == options.end()) {
					//! There is no parent 'first_row_id' to inherit, simply reference the existing column
					return MultiFileReaderVirtualColumnBinding(*row_id_column.get());
				}

				auto computed_row_id =
				    ConstructVirtualRowIdExpression(context, type, entry->second, local_idx.GetIndex() + 1);
				// Create COALESCE(_row_id, computed_row_id)
				auto coalesce_expr = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, type);
				auto file_row_id = make_uniq<BoundReferenceExpression>(type, local_idx.GetIndex());
				coalesce_expr->GetChildrenMutable().push_back(std::move(file_row_id));
				coalesce_expr->GetChildrenMutable().push_back(std::move(computed_row_id));

				vector<idx_t> column_ids;
				column_ids.push_back(i);
				column_ids.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
				return MultiFileReaderVirtualColumnBinding(std::move(coalesce_expr), std::move(column_ids));
			}
		}
		if (entry == options.end()) {
			//! No first-row-id can be found, version must be <3, just return null
			return MultiFileReaderVirtualColumnBinding(Value(LogicalType::BIGINT));
		}

		vector<idx_t> column_ids;
		column_ids.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
		return MultiFileReaderVirtualColumnBinding(
		    ConstructVirtualRowIdExpression(context, type, entry->second, local_idx.GetIndex()), std::move(column_ids));
	}
	if (column_id == COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER) {
		// get the row id start for this file
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("Missing extended info for data file");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("sequence_number");
		for (idx_t i = 0; i < local_columns.size(); i++) {
			auto &col = local_columns[i];
			if (col.identifier.IsNull()) {
				continue;
			}
			if (col.identifier.GetValue<int32_t>() == MultiFileReader::LAST_UPDATED_SEQUENCE_NUMBER_ID) {
				if (entry == options.end()) {
					//! There is no parent 'sequence_number' to inherit, simply reference the existing column
					return MultiFileReaderVirtualColumnBinding(*last_updated_sequence_number_column.get());
				}
				auto &reader = *reader_data.reader;
				// add a projection for the _row_id column we found in the local schema
				reader.column_ids.push_back(MultiFileLocalColumnId(i));
				reader.column_indexes.push_back(ColumnIndex(i));

				auto computed_sequence_number = make_uniq<BoundConstantExpression>(entry->second);
				// Create COALESCE(_last_updated_sequence_number, computed_sequence_number)
				auto coalesce_expr = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, type);
				auto sequence_number = make_uniq<BoundReferenceExpression>(type, local_idx.GetIndex());
				coalesce_expr->GetChildrenMutable().push_back(std::move(sequence_number));
				coalesce_expr->GetChildrenMutable().push_back(std::move(computed_sequence_number));

				vector<idx_t> column_ids;
				column_ids.push_back(i);
				column_ids.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
				return MultiFileReaderVirtualColumnBinding(std::move(coalesce_expr), std::move(column_ids));
			}
		}
		if (entry == options.end()) {
			return MultiFileReaderVirtualColumnBinding(Value(LogicalType::BIGINT));
		}
		return MultiFileReaderVirtualColumnBinding(entry->second);
	}
	return MultiFileReader::GetVirtualColumnExpression(context, reader_data, local_columns, column_id, type, local_idx);
}

vector<PartitionStatistics> IcebergMultiFileReader::IcebergGetPartitionStats(ClientContext &context,
                                                                             GetPartitionStatsInput &input) {
	auto &bind_data = input.bind_data->Cast<MultiFileBindData>();
	vector<PartitionStatistics> result;
	auto &multi_file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
	multi_file_list.GetStatistics(result);
	return result;
}

} // namespace duckdb
