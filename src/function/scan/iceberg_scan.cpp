#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "common/iceberg_utils.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "function/iceberg_functions.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/expression/iceberg_value.hpp"

namespace duckdb {

static void AddNamedParameters(TableFunction &fun) {
	fun.GetSignature().ExtendTypedKwargs([&](TypedKwargs &options) {
		options.Add("allow_moved_paths", LogicalType::BOOLEAN)
		    .Add("mode", LogicalType::VARCHAR)
		    .Add("metadata_compression_codec", LogicalType::VARCHAR)
		    .Add("version", LogicalType::ANY)
		    .Add("version_name_format", LogicalType::VARCHAR)
		    .Add("snapshot_from_timestamp", LogicalType::ANY)
		    .Add("snapshot_from_id", LogicalType::UBIGINT);
	});
}

virtual_column_map_t IcebergVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
	auto result = IcebergTableSchemaVersion::VirtualColumns(file_list.GetScanPlanner().GetMetadata().iceberg_version);
	bind_data.virtual_columns = result;
	return result;
}

static void IcebergScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                 const BoundTableFunction &function) {
	if (!bind_data || !function.function_info) {
		throw NotImplementedException("IcebergScan serialization requires bound scan state");
	}
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	if (!multi_file_data.file_list) {
		throw NotImplementedException("IcebergScan serialization requires a bound file list");
	}
	auto &file_list = multi_file_data.file_list->Cast<IcebergMultiFileList>();
	auto table = file_list.GetTable();
	if (!table) {
		throw NotImplementedException("IcebergScan serialization is only supported for catalog tables");
	}
	auto &scan_info = function.function_info->Cast<IcebergScanInfo>();
	auto &table_info = table->table_info;
	auto qualified_name =
	    QualifiedName(table_info.catalog.GetName(), table_info.schema.name, Identifier(table_info.name));
	serializer.WriteProperty(100, "qualified_name", qualified_name);
	serializer.WriteProperty(101, "schema_id", scan_info.snapshot_info.schema_id);
	const bool has_snapshot = scan_info.snapshot_info.snapshot != nullptr;
	serializer.WriteProperty(102, "has_snapshot", has_snapshot);
	if (has_snapshot) {
		serializer.WriteProperty(103, "snapshot_id", scan_info.snapshot_info.snapshot->snapshot_id);
	}
	serializer.WritePropertyWithDefault<string>(104, "table_uuid", scan_info.metadata.table_uuid);
	serializer.WriteProperty(105, "metadata_path", scan_info.metadata_path);
	serializer.WriteProperty(106, "has_transaction_data", scan_info.transaction_data != nullptr);

	// Filters pushed into the planner by complex filter pushdown. A copy made after filter pushdown restores them so
	// that its file pruning, and the cardinality the optimizer derives from it, match the original scan.
	vector<reference<const IcebergTableFilters::filter_set_t::value_type>> planner_filters;
	for (auto &entry : file_list.GetScanPlanner().Filters()) {
		planner_filters.emplace_back(entry);
	}
	serializer.WriteList(107, "planner_filters", planner_filters.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = planner_filters[i].get();
		list.WriteObject([&](Serializer &object) {
			object.WriteProperty(100, "column_index", entry.first);
			object.WriteProperty(101, "filter", entry.second);
		});
	});
}

//! Plan-copying optimizers such as CTE inlining and partitioned execution only fall back to the original, uncopied
//! plan on NotImplementedException. A scan that cannot be rebound faithfully must therefore throw that type, not fail
//! a query that can still run without the copy.
[[noreturn]] static void ThrowCannotCopyScan(const QualifiedName &qualified_name, const string &reason) {
	throw NotImplementedException("Cannot copy the Iceberg scan of %s: %s", qualified_name.ToString(), reason);
}

static unique_ptr<FunctionData> IcebergScanDeserialize(Deserializer &deserializer, BoundTableFunction &function) {
	auto &context = deserializer.Get<ClientContext &>();
	auto qualified_name = deserializer.ReadProperty<QualifiedName>(100, "qualified_name");
	auto schema_id = deserializer.ReadProperty<int32_t>(101, "schema_id");
	const bool has_snapshot = deserializer.ReadProperty<bool>(102, "has_snapshot");
	optional<int64_t> snapshot_id;
	if (has_snapshot) {
		snapshot_id = deserializer.ReadProperty<int64_t>(103, "snapshot_id");
	}
	auto table_uuid = deserializer.ReadPropertyWithDefault<string>(104, "table_uuid");
	auto metadata_path = deserializer.ReadProperty<string>(105, "metadata_path");
	const bool has_transaction_data = deserializer.ReadProperty<bool>(106, "has_transaction_data");
	IcebergTableFilters planner_filters;
	deserializer.ReadList(107, "planner_filters", [&](Deserializer::List &list, idx_t) {
		list.ReadObject([&](Deserializer &object) {
			auto column_index = object.ReadProperty<ColumnIndex>(100, "column_index");
			auto filter = object.ReadProperty<unique_ptr<TableFilter>>(101, "filter");
			if (!filter || filter->filter_type != TableFilterType::EXPRESSION_FILTER) {
				throw SerializationException("IcebergScan planner filters must be expression filters");
			}
			planner_filters.PushFilter(column_index, unique_ptr_cast<TableFilter, ExpressionFilter>(std::move(filter)));
		});
	});

	EntryLookupInfo current_lookup(CatalogType::TABLE_ENTRY, qualified_name);
	auto catalog_entry = Catalog::GetEntry(context, current_lookup, OnEntryNotFound::RETURN_NULL);
	if (!catalog_entry || catalog_entry->type != CatalogType::TABLE_ENTRY ||
	    catalog_entry->ParentCatalog().GetCatalogType() != "iceberg") {
		ThrowCannotCopyScan(qualified_name, "the table is no longer an Iceberg catalog table");
	}
	auto &table = catalog_entry->Cast<IcebergTableSchemaVersion>().table_info;
	if ((!table_uuid.empty() && table.table_metadata.table_uuid != table_uuid) ||
	    (table_uuid.empty() &&
	     table.table_metadata.GetMetadataPath(FileSystem::GetFileSystem(context)) != metadata_path)) {
		ThrowCannotCopyScan(qualified_name, "the table changed after the scan was bound");
	}

	auto schema_entry = table.schema_versions.find(schema_id);
	if (schema_entry == table.schema_versions.end()) {
		ThrowCannotCopyScan(qualified_name, StringUtil::Format("schema id %d is no longer available", schema_id));
	}

	unique_ptr<BoundAtClause> at_clause;
	if (snapshot_id && !has_transaction_data) {
		at_clause = make_uniq<BoundAtClause>("version", Value::BIGINT(*snapshot_id));
	}
	EntryLookupInfo scan_lookup(CatalogType::TABLE_ENTRY, qualified_name, at_clause.get(), QueryErrorContext());
	unique_ptr<FunctionData> bind_data;
	auto rebound = schema_entry->second->GetScanFunction(context, bind_data, scan_lookup);
	if (!bind_data || !rebound.function_info) {
		ThrowCannotCopyScan(qualified_name, "the table could not be rebound");
	}
	auto &scan_info = rebound.function_info->Cast<IcebergScanInfo>();
	if (scan_info.snapshot_info.schema_id != schema_id ||
	    (scan_info.transaction_data != nullptr) != has_transaction_data ||
	    (scan_info.snapshot_info.snapshot != nullptr) != has_snapshot ||
	    (snapshot_id && scan_info.snapshot_info.snapshot->snapshot_id != *snapshot_id)) {
		ThrowCannotCopyScan(qualified_name, "the rebound scan does not match the bound snapshot and schema");
	}
	if (planner_filters.HasFilters()) {
		// Same replacement MultiFileComplexFilterPushdown performs when pushdown prunes the file list
		auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
		auto &file_list = multi_file_data.file_list->Cast<IcebergMultiFileList>();
		multi_file_data.file_list = file_list.WithPlannerFilters(std::move(planner_filters));
		MultiFileReader::PruneReaders(multi_file_data, *multi_file_data.file_list);
	}
	function.function_info = std::move(rebound.function_info);
	return bind_data;
}

static optional_ptr<TableCatalogEntry> IcebergGetTableEntry(optional_ptr<const FunctionData> bind_data) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &file_list = multi_file_data.file_list->Cast<IcebergMultiFileList>();
	return file_list.GetTable().get();
}

BindInfo IcebergBindInfo(TableFunctionGetBindInfoInput &input) {
	auto &multi_file_data = input.bind_data->Cast<MultiFileBindData>();
	auto &file_list = multi_file_data.file_list->Cast<IcebergMultiFileList>();
	BindInfo result(file_list.GetTable() ? ScanType::TABLE : ScanType::EXTERNAL);

	auto &planner = file_list.GetScanPlanner();
	if (!planner.HasScanInfo()) {
		return result;
	}
	auto &snapshot_info = planner.GetSnapshot();
	auto &table_uuid = planner.GetMetadata().table_uuid;
	if (!table_uuid.empty()) {
		result.InsertOption("table_uuid", Value(table_uuid));
	}
	result.InsertOption("schema_id", Value::INTEGER(snapshot_info.schema_id));
	// Omitted for empty tables, and when uncommitted transaction-local changes are also scanned
	if (snapshot_info.snapshot && !planner.HasTransactionData()) {
		result.InsertOption("snapshot_id", Value::BIGINT(snapshot_info.snapshot->snapshot_id));
	}
	return result;
}

static void IcebergSetScanOrder(unique_ptr<RowGroupOrderOptions> order_options, optional_ptr<FunctionData> bind_data) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &file_list = multi_file_data.file_list->Cast<IcebergMultiFileList>();
	file_list.GetScanPlanner().SetScanOrder(std::move(order_options));
}

//! FIXME: needs v1.5.1, causes a crash on v1.5.0
// static bool IcebergScanSupportsPushdownType(const FunctionData &bind_data_p, idx_t column_id) {
//	// Don't push down filters on the _row_id virtual column
//	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
//		return false;
//	}

//	// Default behavior for other columns
//	return true;
//}

TableFunctionSet IcebergFunctions::GetIcebergScanFunction(ExtensionLoader &loader) {
	// The iceberg_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
	// IcebergMultiFileReader into it to create a Iceberg-based multi file read

	auto &parquet_scan = loader.GetTableFunction("parquet_scan");
	auto parquet_scan_copy = parquet_scan.functions;

	parquet_scan_copy.ApplyToFunctions([](TableFunction &function) {
		// Register the MultiFileReader as the driver for reads
		function.get_multi_file_reader = IcebergMultiFileReader::CreateInstance;
		function.late_materialization = false;

		// Catalog-backed scans serialize by rebinding the table; path-based scans throw NotImplementedException
		function.serialize = IcebergScanSerialize;
		function.deserialize = IcebergScanDeserialize;

		// Unset these: they are either broken or very inefficient.
		// TODO: implement/fix these
		function.statistics = nullptr;
		function.table_scan_progress = nullptr;

		function.get_bind_info = IcebergBindInfo;
		function.get_table_entry = IcebergGetTableEntry;
		function.get_virtual_columns = IcebergVirtualColumns;
		function.get_partition_stats = IcebergMultiFileReader::IcebergGetPartitionStats;
		function.set_scan_order = IcebergSetScanOrder;
		// function.supports_pushdown_type = IcebergScanSupportsPushdownType;

		// Schema param is just confusing here, so the options are rebuilt without it
		auto &signature = function.GetSignature();
		signature.ExtendTypedKwargs([](TypedKwargs &options) {
			TypedKwargs without_schema;
			for (auto &option : options.GetOptions()) {
				if (option.name == "schema") {
					continue;
				}
				without_schema.Add(option.name, option.type);
				for (auto &alias : option.aliases) {
					without_schema.Alias(alias);
				}
			}
			options = std::move(without_schema);
		});
		AddNamedParameters(function);

		function.SetName("iceberg_scan");
	});

	parquet_scan_copy.SetName("iceberg_scan");
	return parquet_scan_copy;
}

} // namespace duckdb
