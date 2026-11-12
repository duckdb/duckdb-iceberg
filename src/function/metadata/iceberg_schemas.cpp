#include <algorithm>

#include "duckdb/main/client_context.hpp"

#include "function/iceberg_functions.hpp"
#include "iceberg_options.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

struct IcebergSchemaFieldEntry {
	int32_t schema_id;
	string name;
	int32_t field_id;
	//! The DuckDB type the field is read as (BIGINT, DECIMAL(15,2), STRUCT(...)), as in iceberg_column_stats.
	//! This is intentionally not the Iceberg type name (long, decimal(15, 2), struct<...>): the DuckDB type keeps
	//! the full nested type, and matches what DESCRIBE shows. Iceberg types without a DuckDB equivalent use the
	//! extension's own types, e.g. fixed[5] is shown as ICEBERG_FIXED(5).
	string type;
	bool required;
	//! NULL when the field has no doc
	Value doc;
	bool is_current;
};

//! One entry per top-level field of every schema, ordered by schema id and then by field order
static vector<IcebergSchemaFieldEntry> GetSchemaFields(const IcebergTableMetadata &metadata) {
	vector<reference<const IcebergTableSchema>> schemas;
	metadata.GetSchemas().ForEachSchema([&](const IcebergTableSchema &schema) { schemas.push_back(schema); });
	std::sort(schemas.begin(), schemas.end(),
	          [](const IcebergTableSchema &a, const IcebergTableSchema &b) { return a.schema_id < b.schema_id; });

	auto current_schema_id = metadata.GetCurrentSchemaId();
	vector<IcebergSchemaFieldEntry> result;
	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();
		bool is_current = schema.schema_id == current_schema_id;
		for (auto &column : schema.columns) {
			result.push_back(IcebergSchemaFieldEntry {
			    schema.schema_id, column->name, column->id, column->type.ToString(), column->required,
			    column->doc ? Value(*column->doc) : Value(LogicalType::VARCHAR), is_current});
		}
	}
	return result;
}

struct IcebergSchemasBindData : public TableFunctionData {
	IcebergSchemasBindData() {};
	IcebergTableMetadata metadata {IcebergTableMetadataSchemas {}};
};

struct IcebergSchemasGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<IcebergSchemasBindData>();
		auto global_state = make_uniq<IcebergSchemasGlobalState>();
		global_state->fields = GetSchemaFields(bind_data.metadata);
		return std::move(global_state);
	}

	vector<IcebergSchemaFieldEntry> fields;
	//! Position in the list of schema fields
	idx_t offset = 0;
};

static unique_ptr<FunctionData> IcebergSchemasBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	IcebergOptions options(input.named_parameters);
	auto input_string = input.inputs[0].ToString();
	auto bind_data = make_uniq<IcebergSchemasBindData>();
	bind_data->metadata = std::move(IcebergUtils::ResolveTableMetadata(context, input_string, options).metadata);

	names.emplace_back("schema_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("field_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("required");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("doc");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("is_current");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(bind_data);
}

static void IcebergSchemasFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<IcebergSchemasGlobalState>();

	idx_t out = 0;
	while (global_state.offset < global_state.fields.size() && out < STANDARD_VECTOR_SIZE) {
		auto &field = global_state.fields[global_state.offset++];

		idx_t col = 0;
		output.data[col++].SetValue(out, Value::INTEGER(field.schema_id));
		output.data[col++].SetValue(out, Value(field.name));
		output.data[col++].SetValue(out, Value::INTEGER(field.field_id));
		output.data[col++].SetValue(out, Value(field.type));
		output.data[col++].SetValue(out, Value::BOOLEAN(field.required));
		output.data[col++].SetValue(out, field.doc);
		output.data[col++].SetValue(out, Value::BOOLEAN(field.is_current));
		out++;
	}
	output.SetChildCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergSchemasFunction() {
	TableFunctionSet function_set("iceberg_schemas");
	TableFunction table_function(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                             IcebergSchemasFunction, IcebergSchemasBind, IcebergSchemasGlobalState::Init);
	table_function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("metadata_compression_codec", LogicalType::VARCHAR)
		    .Add("version", LogicalType::ANY)
		    .Add("version_name_format", LogicalType::VARCHAR);
	});
	function_set.AddFunction(table_function);
	return function_set;
}

} // namespace duckdb
