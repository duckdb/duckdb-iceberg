#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
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
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

#include <numeric>

namespace duckdb {

static vector<LogicalType> IcebergManifestEntryTypes() {
	return {
	    //! status
	    LogicalType::VARCHAR,
	    //! content
	    LogicalType::VARCHAR,
	    //! file_path
	    LogicalType::VARCHAR,
	    //! file_format
	    LogicalType::VARCHAR,
	    //! record_count
	    LogicalType::BIGINT,
	    //! data_sequence_number
	    LogicalType::BIGINT,
	    //! file_sequence_number
	    LogicalType::BIGINT,
	};
}

static vector<string> IcebergManifestEntryNames() {
	return {"status",
	        "content",
	        "file_path",
	        "file_format",
	        "record_count",
	        "data_sequence_number",
	        "file_sequence_number"};
}

static vector<LogicalType> IcebergManifestTypes() {
	return {
	    //! manifest_path
	    LogicalType::VARCHAR,
	    //! manifest_sequence_number
	    LogicalType::BIGINT,
	    //! manifest_content
	    LogicalType::VARCHAR,
	};
}

static vector<string> IcebergManifestNames() {
	return {"manifest_path", "manifest_sequence_number", "manifest_content"};
}

struct IcebergMetaDataBindData : public TableFunctionData {
	unique_ptr<IcebergManifestList> iceberg_table;
};

struct IcebergMetaDataGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	IcebergMetaDataGlobalTableFunctionState() {

	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergMetaDataGlobalTableFunctionState>();
	}

	idx_t current_manifest_idx = 0;
	idx_t current_manifest_entry_idx = 0;
};

static unique_ptr<FunctionData> IcebergMetaDataBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// return a TableRef that contains the scans for the
	auto ret = make_uniq<IcebergMetaDataBindData>();

	IcebergOptions options(input.named_parameters);
	auto input_string = input.inputs[0].ToString();
	auto resolved_metadata = IcebergUtils::ResolveTableMetadata(context, input_string, options);

	auto snapshot_to_scan = resolved_metadata.metadata.GetSnapshot(*options.snapshot_lookup);

	if (snapshot_to_scan.snapshot) {
		ret->iceberg_table = IcebergManifestList::Load(resolved_metadata.table_location, resolved_metadata.metadata,
		                                               snapshot_to_scan, context, options);
	}

	auto manifest_types = IcebergManifestTypes();
	return_types.insert(return_types.end(), manifest_types.begin(), manifest_types.end());
	auto manifest_entry_types = IcebergManifestEntryTypes();
	return_types.insert(return_types.end(), manifest_entry_types.begin(), manifest_entry_types.end());

	auto manifest_names = IcebergManifestNames();
	names.insert(names.end(), manifest_names.begin(), manifest_names.end());
	auto manifest_entry_names = IcebergManifestEntryNames();
	names.insert(names.end(), manifest_entry_names.begin(), manifest_entry_names.end());

	D_ASSERT(manifest_types.size() == manifest_names.size());

	return std::move(ret);
}

static void AddString(Vector &vec, idx_t index, string_t &&str) {
	FlatVector::GetDataMutable<string_t>(vec)[index] = StringVector::AddString(vec, std::move(str));
}

static void IcebergMetaDataFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergMetaDataBindData>();
	auto &global_state = data.global_state->Cast<IcebergMetaDataGlobalTableFunctionState>();

	if (!bind_data.iceberg_table) {
		//! Table is empty
		return;
	}

	idx_t out = 0;
	auto &table_entries = bind_data.iceberg_table->GetManifestFilesConst();
	for (; global_state.current_manifest_idx < table_entries.size(); global_state.current_manifest_idx++) {
		auto &table_entry = table_entries[global_state.current_manifest_idx];
		auto &entries = table_entry.GetManifestEntries();
		for (; global_state.current_manifest_entry_idx < entries.size(); global_state.current_manifest_entry_idx++) {
			if (out >= STANDARD_VECTOR_SIZE) {
				output.SetChildCardinality(out);
				return;
			}
			auto &manifest = table_entry.file;
			auto &manifest_entry = entries[global_state.current_manifest_entry_idx];
			auto &data_file = manifest_entry.data_file;

			//! manifest_path
			AddString(output.data[0], out, string_t(manifest.manifest_path));
			//! manifest_sequence_number
			if (!manifest.sequence_number) {
				throw InvalidConfigurationException("manifest_file.sequence_number is not set");
			}
			FlatVector::GetDataMutable<int64_t>(output.data[1])[out] = *manifest.sequence_number;
			//! manifest_content
			AddString(output.data[2], out, string_t(IcebergManifestContentTypeToString(manifest.content)));

			//! status
			AddString(output.data[3], out, string_t(IcebergManifestEntryStatusTypeToString(manifest_entry.status)));
			//! content
			AddString(output.data[4], out, string_t(IcebergManifestEntryContentTypeToString(data_file.content)));
			//! file_path
			AddString(output.data[5], out, string_t(data_file.file_path));
			//! file_format
			AddString(output.data[6], out, string_t(data_file.file_format));
			//! record_count
			FlatVector::GetDataMutable<int64_t>(output.data[7])[out] = data_file.record_count;
			FlatVector::GetDataMutable<int64_t>(output.data[8])[out] = manifest_entry.GetSequenceNumber(manifest);
			FlatVector::GetDataMutable<int64_t>(output.data[9])[out] = manifest_entry.GetFileSequenceNumber(manifest);
			out++;
		}
		global_state.current_manifest_entry_idx = 0;
	}
	output.SetChildCardinality(out);
}

TableFunctionSet IcebergFunctions::GetIcebergMetadataFunction() {
	TableFunctionSet function_set("iceberg_metadata");

	auto fun = TableFunction({LogicalType::VARCHAR}, IcebergMetaDataFunction, IcebergMetaDataBind,
	                         IcebergMetaDataGlobalTableFunctionState::Init);
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP_MS;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
