#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binder.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "function/iceberg_functions.hpp"

namespace duckdb {

namespace {

struct RefFunctionInfo : public TableFunctionInfo {
	RefFunctionInfo(string function_name_p, bool is_create_p, bool is_branch_p)
	    : function_name(std::move(function_name_p)), is_create(is_create_p), is_branch(is_branch_p) {
	}

	string function_name;
	//! Whether the function creates or drops the ref
	bool is_create;
	//! Whether the function works on branches or on tags
	bool is_branch;
};

struct RefFunctionBindData : public TableFunctionData {
	optional_ptr<IcebergTableSchemaVersion> iceberg_table;
	optional_ptr<const RefFunctionInfo> info;
	string ref_name;
	//! The snapshot a created ref points at; main's current snapshot if not given
	optional<int64_t> snapshot_id;
};

struct RefFunctionGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<RefFunctionGlobalState>();
	}

	bool finished = false;
};

//! The ref a function created or dropped
struct RefResult {
	string name;
	string type;
	int64_t snapshot_id = 0;
};

static string RefTypeName(bool is_branch) {
	return is_branch ? "branch" : "tag";
}

static bool CheckTableIsIcebergTable(optional_ptr<CatalogEntry> entry) {
	auto &catalog_entry = entry->Cast<InCatalogEntry>();
	auto &catalog = catalog_entry.catalog;
	if (catalog.GetCatalogType() != "iceberg") {
		return false;
	}
	if (entry->type != CatalogType::TABLE_ENTRY) {
		return false;
	}
	return true;
}

static void VerifyInputIsNotAFile(ClientContext &context, string &input_string, const string &function_name) {
	auto qualified_name = QualifiedName::ParseComponents(input_string);
	if (qualified_name.size() == 1) {
		auto &fs = FileSystem::GetFileSystem(context);
		if (fs.DirectoryExists(input_string) || fs.FileExists(input_string)) {
			throw InvalidInputException("Cannot call %s() on a file/directory", function_name);
		}
	}
}

static unique_ptr<FunctionData> RefFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &info = input.info->Cast<RefFunctionInfo>();
	auto result = make_uniq<RefFunctionBindData>();
	result->info = &info;

	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("%s: table_name cannot be NULL", info.function_name);
	}
	auto input_string = input.inputs[0].ToString();
	VerifyInputIsNotAFile(context, input_string, info.function_name);
	auto iceberg_table = IcebergUtils::GetTableEntry(context, input_string);
	if (!CheckTableIsIcebergTable(iceberg_table)) {
		throw InvalidInputException("Cannot call %s on non-iceberg table", info.function_name);
	}
	result->iceberg_table = iceberg_table->Cast<IcebergTableSchemaVersion>();

	if (input.inputs[1].IsNull()) {
		throw InvalidInputException("%s: %s cannot be NULL", info.function_name,
		                            info.is_branch ? "branch_name" : "tag_name");
	}
	result->ref_name = StringValue::Get(input.inputs[1]);
	if (result->ref_name.empty()) {
		throw InvalidInputException("%s: Branch or tag name cannot be empty", info.function_name);
	}
	if (input.inputs.size() > 2) {
		if (input.inputs[2].IsNull()) {
			throw InvalidInputException("%s: snapshot_id cannot be NULL", info.function_name);
		}
		result->snapshot_id = input.inputs[2].DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
	}

	if (input.binder) {
		DatabaseModificationType modification;
		modification |= DatabaseModificationType::ALTER_TABLE;
		input.binder->GetStatementProperties().RegisterDBModify(iceberg_table->ParentCatalog(), context, modification);
	}

	names.emplace_back("name");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("type");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("snapshot_id");
	return_types.push_back(LogicalType::BIGINT);
	return std::move(result);
}

static RefResult CreateRef(const RefFunctionBindData &bind_data, IcebergTableMetadata &metadata,
                           IcebergTransactionData &transaction_data) {
	auto &name = bind_data.ref_name;
	auto is_branch = bind_data.info->is_branch;
	//! Metadata without 'refs' still has a main branch at the current snapshot.
	if (metadata.FindRef(name) || (name == "main" && metadata.current_snapshot_id)) {
		throw InvalidInputException("Ref %s already exists", name);
	}
	if (name == "main") {
		//! Only a table without a current snapshot has no main branch, and creating one would set its current snapshot
		throw InvalidInputException("%s cannot create the main branch", bind_data.info->function_name);
	}
	rest_api_objects::SnapshotReference ref;
	ref.type = RefTypeName(is_branch);
	if (bind_data.snapshot_id) {
		ref.snapshot_id = *bind_data.snapshot_id;
		if (!metadata.FindSnapshotByIdInternal(ref.snapshot_id)) {
			throw InvalidInputException("Cannot set %s to unknown snapshot: %d", name, ref.snapshot_id);
		}
	} else if (metadata.current_snapshot_id) {
		ref.snapshot_id = *metadata.current_snapshot_id;
	} else {
		throw InvalidInputException("Cannot create %s %s: the table has no current snapshot", RefTypeName(is_branch),
		                            name);
	}
	transaction_data.TableCreateRef(metadata, name, ref);
	return RefResult {name, ref.type, ref.snapshot_id};
}

static RefResult DropRef(const RefFunctionBindData &bind_data, IcebergTableMetadata &metadata,
                         IcebergTransactionData &transaction_data) {
	auto &name = bind_data.ref_name;
	auto is_branch = bind_data.info->is_branch;
	if (is_branch && name == "main") {
		throw InvalidInputException("Cannot remove main branch");
	}
	auto ref = metadata.FindRef(name);
	//! Metadata without 'refs' still has a main branch at the current snapshot
	auto implicit_main = !ref && name == "main" && metadata.current_snapshot_id;
	if (!ref && !implicit_main) {
		throw InvalidInputException("%s does not exist: %s", is_branch ? "Branch" : "Tag", name);
	}
	auto ref_is_branch = implicit_main || StringUtil::CIEquals(ref->type, "branch");
	if (ref_is_branch != is_branch) {
		throw InvalidInputException("Ref %s is a %s not a %s", name, RefTypeName(ref_is_branch),
		                            RefTypeName(is_branch));
	}
	RefResult result {name, ref->type, ref->snapshot_id};
	transaction_data.TableDropRef(metadata, name);
	return result;
}

static void RefFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RefFunctionBindData>();
	auto &global_state = data.global_state->Cast<RefFunctionGlobalState>();
	if (!bind_data.iceberg_table || global_state.finished) {
		output.SetChildCardinality(0);
		return;
	}

	auto &table_info = bind_data.iceberg_table->table_info;
	auto &iceberg_transaction = IcebergTransaction::Get(context, bind_data.iceberg_table->catalog);
	RefResult result;
	ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTable &table) {
		auto &transaction_data = table.GetOrCreateTransactionData(iceberg_transaction);
		auto &metadata = table.GetMetadataMutable();
		if (bind_data.info->is_create) {
			result = CreateRef(bind_data, metadata, transaction_data);
		} else {
			result = DropRef(bind_data, metadata, transaction_data);
		}
	});

	output.data[0].SetValue(0, Value(result.name));
	output.data[1].SetValue(0, Value(StringUtil::Upper(result.type)));
	output.data[2].SetValue(0, Value::BIGINT(result.snapshot_id));
	output.SetChildCardinality(1);
	global_state.finished = true;
}

static TableFunction CreateRefFunction(const shared_ptr<RefFunctionInfo> &info,
                                       optional<LogicalType> snapshot_id_type) {
	FunctionSignature signature;
	signature.AddPositionalOnly("table_name", LogicalType::VARCHAR);
	signature.AddPositionalOnly(info->is_branch ? "branch_name" : "tag_name", LogicalType::VARCHAR);
	if (snapshot_id_type) {
		signature.AddPositionalOnly("snapshot_id", *snapshot_id_type);
	}
	TableFunction function(std::move(signature), RefFunction, RefFunctionBind, RefFunctionGlobalState::Init);
	function.function_info = info;
	return function;
}

static TableFunctionSet CreateRefFunctionSet(const string &function_name, bool is_create, bool is_branch) {
	auto info = make_shared_ptr<RefFunctionInfo>(function_name, is_create, is_branch);
	TableFunctionSet function_set {Identifier(function_name)};
	function_set.AddFunction(CreateRefFunction(info, optional<LogicalType>()));
	if (is_create) {
		//! iceberg_snapshots returns snapshot ids as UBIGINT, so take those without a cast
		function_set.AddFunction(CreateRefFunction(info, LogicalType::BIGINT));
		function_set.AddFunction(CreateRefFunction(info, LogicalType::UBIGINT));
	}
	return function_set;
}

} // namespace

vector<TableFunctionSet> IcebergFunctions::GetIcebergBranchTagFunctions() {
	vector<TableFunctionSet> result;
	result.push_back(CreateRefFunctionSet("iceberg_create_branch", true, true));
	result.push_back(CreateRefFunctionSet("iceberg_create_tag", true, false));
	result.push_back(CreateRefFunctionSet("iceberg_drop_branch", false, true));
	result.push_back(CreateRefFunctionSet("iceberg_drop_tag", false, false));
	return result;
}

} // namespace duckdb
