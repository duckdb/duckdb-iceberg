#include "duckdb/main/client_context.hpp"

#include "function/iceberg_functions.hpp"
#include "iceberg_options.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

struct IcebergPartitionSpecEntry {
	int32_t spec_id;
	bool is_default;
	int64_t field_id;
	string name;
	string transform;
	int64_t source_id;
	//! NULL when no schema has the source column
	Value source_column;
};

//! One entry per partition field of every spec, ordered by spec id; an unpartitioned spec has no fields
static vector<IcebergPartitionSpecEntry> GetPartitionSpecFields(const IcebergTableMetadata &metadata) {
	vector<int32_t> spec_ids;
	for (auto &entry : metadata.GetPartitionSpecs()) {
		spec_ids.push_back(entry.first);
	}
	std::sort(spec_ids.begin(), spec_ids.end());

	auto &current_schema = metadata.GetLatestSchema();
	vector<IcebergPartitionSpecEntry> result;
	for (auto spec_id : spec_ids) {
		auto &spec = metadata.GetPartitionSpecs().at(spec_id);
		for (auto &field : spec.fields) {
			//! The current name of the source column, or its last name if it has since been dropped
			auto column = current_schema.TryGetColumnByFieldId(field.source_id);
			if (!column) {
				column = metadata.FindColumnByFieldId(static_cast<int32_t>(field.source_id));
			}
			result.push_back(IcebergPartitionSpecEntry {
			    spec_id, spec_id == metadata.default_spec_id, static_cast<int64_t>(field.partition_field_id),
			    field.GetPartitionSpecFieldName(), field.transform.RawType(), static_cast<int64_t>(field.source_id),
			    column ? Value(column->name) : Value(LogicalType::VARCHAR)});
		}
	}
	return result;
}

struct IcebergPartitionSpecsBindData : public TableFunctionData {
	IcebergPartitionSpecsBindData() {};
	IcebergTableMetadata metadata {IcebergTableMetadataSchemas {}};
};

struct IcebergPartitionSpecsGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<IcebergPartitionSpecsBindData>();
		auto global_state = make_uniq<IcebergPartitionSpecsGlobalState>();
		global_state->fields = GetPartitionSpecFields(bind_data.metadata);
		return std::move(global_state);
	}

	vector<IcebergPartitionSpecEntry> fields;
	//! Position in the list of partition fields
	idx_t offset = 0;
};

static unique_ptr<FunctionData> IcebergPartitionSpecsBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types,
                                                          vector<Identifier> &names) {
	IcebergOptions options(input.named_parameters);
	auto input_string = input.inputs[0].ToString();
	auto bind_data = make_uniq<IcebergPartitionSpecsBindData>();
	bind_data->metadata = std::move(IcebergUtils::ResolveTableMetadata(context, input_string, options).metadata);

	names.emplace_back("spec_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("is_default");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("field_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("transform");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("source_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("source_column");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(bind_data);
}

static void IcebergPartitionSpecsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<IcebergPartitionSpecsGlobalState>();

	idx_t out = 0;
	while (global_state.offset < global_state.fields.size() && out < STANDARD_VECTOR_SIZE) {
		auto &field = global_state.fields[global_state.offset++];

		idx_t col = 0;
		output.data[col++].SetValue(out, Value::INTEGER(field.spec_id));
		output.data[col++].SetValue(out, Value::BOOLEAN(field.is_default));
		output.data[col++].SetValue(out, Value::BIGINT(field.field_id));
		output.data[col++].SetValue(out, Value(field.name));
		output.data[col++].SetValue(out, Value(field.transform));
		output.data[col++].SetValue(out, Value::BIGINT(field.source_id));
		output.data[col++].SetValue(out, field.source_column);
		out++;
	}
	output.SetChildCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergPartitionSpecsFunction() {
	TableFunctionSet function_set("iceberg_partition_specs");
	TableFunction table_function(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                             IcebergPartitionSpecsFunction, IcebergPartitionSpecsBind,
	                             IcebergPartitionSpecsGlobalState::Init);
	table_function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("metadata_compression_codec", LogicalType::VARCHAR)
		    .Add("version", LogicalType::ANY)
		    .Add("version_name_format", LogicalType::VARCHAR);
	});
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
