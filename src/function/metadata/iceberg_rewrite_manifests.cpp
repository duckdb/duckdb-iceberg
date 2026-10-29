#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "catalog/rest/api/iceberg_manifest_merge.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "function/iceberg_functions.hpp"
#include "maintenance/rewrite_manifests.hpp"

namespace duckdb {

namespace {

struct IcebergRewriteManifestsBindData : public TableFunctionData {
	optional_ptr<IcebergTableSchemaVersion> iceberg_table;
	optional<int32_t> spec_id;
	optional<vector<string>> sort_by;
	optional<int64_t> target_manifest_size_bytes;
};

struct IcebergRewriteManifestsGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<IcebergRewriteManifestsGlobalState>();
	}

	bool finished = false;
};

static int64_t ParseTargetManifestSizeBytes(const Value &value) {
	if (value.IsNull()) {
		throw InvalidInputException("iceberg_rewrite_manifests: 'target_manifest_size_bytes' cannot be NULL");
	}
	if (value.type().id() == LogicalTypeId::VARCHAR) {
		idx_t parsed_value;
		auto error = StringUtil::TryParseFormattedBytes(StringValue::Get(value), parsed_value);
		if (!error.empty()) {
			throw InvalidInputException("iceberg_rewrite_manifests: invalid 'target_manifest_size_bytes': %s", error);
		}
		if (parsed_value == 0 || parsed_value > static_cast<idx_t>(NumericLimits<int64_t>::Maximum())) {
			throw InvalidInputException("iceberg_rewrite_manifests: 'target_manifest_size_bytes' is out of range");
		}
		return static_cast<int64_t>(parsed_value);
	}
	auto numeric_value = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
	if (numeric_value <= 0) {
		throw InvalidInputException("iceberg_rewrite_manifests: 'target_manifest_size_bytes' must be > 0, got %lld",
		                            numeric_value);
	}
	return numeric_value;
}

static unique_ptr<FunctionData> IcebergRewriteManifestsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	auto ret = make_uniq<IcebergRewriteManifestsBindData>();

	auto input_string = input.inputs[0].ToString();
	auto table_entry = IcebergUtils::GetTableEntry(context, input_string);
	auto &catalog = table_entry->Cast<InCatalogEntry>().catalog;
	if (catalog.GetCatalogType() != "iceberg" || table_entry->type != CatalogType::TABLE_ENTRY) {
		throw InvalidInputException("Cannot call iceberg_rewrite_manifests on non-iceberg table");
	}
	ret->iceberg_table = table_entry->Cast<IcebergTableSchemaVersion>();

	for (auto &kv : input.named_parameters) {
		auto option = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &value = kv.second;
		if (option == "spec_id") {
			if (value.IsNull()) {
				throw InvalidInputException("iceberg_rewrite_manifests: 'spec_id' cannot be NULL");
			}
			ret->spec_id = value.GetValue<int32_t>();
		} else if (option == "sort_by") {
			if (value.IsNull()) {
				throw InvalidInputException("iceberg_rewrite_manifests: 'sort_by' cannot be NULL");
			}
			vector<string> sort_by;
			for (auto &child : ListValue::GetChildren(value)) {
				if (child.IsNull()) {
					throw InvalidInputException("iceberg_rewrite_manifests: 'sort_by' cannot contain NULL");
				}
				sort_by.push_back(StringValue::Get(child));
			}
			ret->sort_by = std::move(sort_by);
		} else if (option == "target_manifest_size_bytes") {
			ret->target_manifest_size_bytes = ParseTargetManifestSizeBytes(value);
		}
	}

	if (input.binder) {
		DatabaseModificationType modification;
		modification |= DatabaseModificationType::ALTER_TABLE;
		input.binder->GetStatementProperties().RegisterDBModify(table_entry->ParentCatalog(), context, modification);
	}

	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("rewritten_manifests_count");
	names.emplace_back("added_manifests_count");
	return std::move(ret);
}

static RewriteManifestsSpec ResolveRewriteManifestsSpec(ClientContext &context,
                                                        const IcebergRewriteManifestsBindData &bind_data,
                                                        const IcebergTableMetadata &metadata) {
	RewriteManifestsSpec spec;
	spec.spec_id = bind_data.spec_id.value_or(metadata.default_spec_id);
	auto &partition_specs = metadata.GetPartitionSpecs();
	auto spec_it = partition_specs.find(spec.spec_id);
	if (spec_it == partition_specs.end()) {
		throw InvalidInputException("iceberg_rewrite_manifests: partition spec %d does not exist", spec.spec_id);
	}
	auto &fields = spec_it->second.GetFields();
	auto &schema = metadata.GetLatestSchema();
	if (bind_data.sort_by) {
		//! Spark names the partition fields of the spec being rewritten. Spark names an identity field after
		//! its source column, so accept that too.
		for (auto &name : *bind_data.sort_by) {
			optional_ptr<const IcebergPartitionSpecField> match;
			for (auto &field : fields) {
				if (StringUtil::CIEquals(field.GetPartitionSpecFieldName(), name)) {
					match = &field;
					break;
				}
			}
			for (auto &field : fields) {
				if (match) {
					break;
				}
				if (field.transform.Type() != IcebergTransformType::IDENTITY) {
					continue;
				}
				auto source_column = schema.TryGetColumnByFieldId(field.source_id);
				if (source_column && StringUtil::CIEquals(source_column->name, name)) {
					match = &field;
				}
			}
			if (!match) {
				throw InvalidInputException(
				    "iceberg_rewrite_manifests: '%s' is not a partition field of partition spec %d", name,
				    spec.spec_id);
			}
			spec.sort_field_ids.push_back(match->partition_field_id);
		}
	} else {
		for (auto &field : fields) {
			spec.sort_field_ids.push_back(field.partition_field_id);
		}
	}
	spec.target_manifest_size_bytes = bind_data.target_manifest_size_bytes.value_or(
	    IcebergManifestMergeConfig::FromTableMetadata(metadata, context).target_size_bytes);
	return spec;
}

static void IcebergRewriteManifestsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergRewriteManifestsBindData>();
	auto &global_state = data.global_state->Cast<IcebergRewriteManifestsGlobalState>();
	if (global_state.finished) {
		output.SetChildCardinality(0);
		return;
	}

	auto &table_info = bind_data.iceberg_table->table_info;
	RewriteManifestsResult result;
	auto &iceberg_transaction = IcebergTransaction::Get(context, bind_data.iceberg_table->catalog);
	ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTable &table) {
		auto &metadata = table.table_metadata;
		if (metadata.iceberg_version < 2) {
			throw NotImplementedException("iceberg_rewrite_manifests on Iceberg V%d tables", metadata.iceberg_version);
		}
		table.VerifyWriteEncryptionSupported();
		auto spec = ResolveRewriteManifestsSpec(context, bind_data, metadata);
		//! Install vended storage secrets before reading the manifest list from object storage.
		table.LoadCredentials(context);
		auto &transaction_data = table.GetOrCreateTransactionData(iceberg_transaction);
		result = transaction_data.AddRewriteManifests(spec);
	});

	FlatVector::GetDataMutable<int64_t>(output.data[0])[0] = NumericCast<int64_t>(result.rewritten_manifests_count);
	FlatVector::GetDataMutable<int64_t>(output.data[1])[0] = NumericCast<int64_t>(result.added_manifests_count);
	output.SetChildCardinality(1);
	global_state.finished = true;
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRewriteManifestsFunction() {
	TableFunctionSet function_set("iceberg_rewrite_manifests");
	TableFunction function(FunctionSignature().AddPositionalOnly("table_name", LogicalType::VARCHAR),
	                       IcebergRewriteManifestsFunction, IcebergRewriteManifestsBind,
	                       IcebergRewriteManifestsGlobalState::Init);
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("spec_id", LogicalType::INTEGER)
		    .Add("sort_by", LogicalType::LIST(LogicalType::VARCHAR))
		    .Add("target_manifest_size_bytes", LogicalType::ANY);
	});
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
