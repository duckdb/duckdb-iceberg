#include "catalog/rest/iceberg_rest_commit.hpp"

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/logger.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "catalog/rest/iceberg_rest_backend.hpp"
#include "catalog/rest/iceberg_table_metadata_cache.hpp"
#include "common/iceberg_constants.hpp"
#include "iceberg_logging.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

template <class RESTObject>
static string RESTObjectToJSONString(const RESTObject &object) {
	JSONWriter writer;
	writer.SetRoot(object.ToJSON(writer));
	return writer.ToString(JSONWriteFlags::ALLOW_INF_AND_NAN);
}

IcebergRestCommit::IcebergRestCommit(IcebergRestBackend &backend, ClientContext &context, IcebergCommitPlan &plan)
    : backend(backend), context(context), plan(plan) {
}

void IcebergRestCommit::Run() {
	CreateNamespaces();
	UpdateNamespaceProperties();
	CommitTables();
	DropViews();
	CreateViews();
	DropNamespaces();
}

//===--------------------------------------------------------------------===//
// Namespaces
//===--------------------------------------------------------------------===//

void IcebergRestCommit::CreateNamespaces() {
	auto &namespaces = plan.CreatedNamespaces();
	for (idx_t i = 0; i < namespaces.size(); i++) {
		auto &ns = namespaces[i];
		rest_api_objects::CreateNamespaceRequest request;
		request._namespace.value = ns.levels;
		request.properties = case_insensitive_map_t<string>();
		auto create_body = RESTObjectToJSONString(request);

		bool created = IRCAPI::CommitNamespaceCreate(context, backend, ns.ToString(), create_body);
		plan.OnNamespaceCreated(i, !created);
	}
}

void IcebergRestCommit::UpdateNamespaceProperties() {
	for (auto &update : plan.NamespacePropertyUpdates()) {
		rest_api_objects::UpdateNamespacePropertiesRequest request;
		request.removals = std::move(update.removals);
		request.updates = std::move(update.updates);
		auto body = RESTObjectToJSONString(request);

		IRCAPI::CommitNamespacePropertiesUpdate(context, backend, body, update.ns.levels);
	}
}

void IcebergRestCommit::DropNamespaces() {
	auto &namespaces = plan.DroppedNamespaces();
	for (idx_t i = 0; i < namespaces.size(); i++) {
		IRCAPI::CommitNamespaceDrop(context, backend, namespaces[i].levels);
		plan.OnNamespaceDropped(i);
	}
	plan.OnNamespacesDropped();
}

//===--------------------------------------------------------------------===//
// Tables
//===--------------------------------------------------------------------===//

void IcebergRestCommit::CommitTables() {
	switch (plan.TableChangeKind()) {
	case IcebergTableChangeKind::NONE:
		return;
	case IcebergTableChangeKind::UPDATE:
		CommitTableUpdates();
		return;
	case IcebergTableChangeKind::DROP: {
		auto table = plan.DroppedTable();
		IRCAPI::CommitTableDelete(context, backend, table.ns.levels, table.name);
		plan.OnTableDropped(context);
		return;
	}
	case IcebergTableChangeKind::RENAME: {
		auto table = plan.RenamedTable();
		rest_api_objects::RenameTableRequest request;
		request.source._namespace.value = table.ns.levels;
		request.source.name = table.name;
		request.destination._namespace.value = table.ns.levels;
		request.destination.name = plan.NewTableName();
		IRCAPI::CommitTableRename(context, backend, RESTObjectToJSONString(request));
		plan.OnTableRenamed();
		return;
	}
	}
	throw InternalException("Unknown IcebergTableChangeKind");
}

void IcebergRestCommit::CommitTableUpdates() {
	if (!plan.HasTableUpdates()) {
		return;
	}
	if (plan.CanCommitTablesAtomically()) {
		IcebergCommitRetryLoop::Run(
		    context, plan, [&](IcebergStagedCommit &staged) { return plan.StageTables(context, staged); },
		    [&](IcebergStagedCommit &staged) { return PublishTransaction(staged); });
	} else {
		D_ASSERT(backend.supported_urls.count("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
		IcebergCommitRetryLoop::Run(
		    context, plan, [&](IcebergStagedCommit &staged) { return plan.StageTable(context, staged); },
		    [&](IcebergStagedCommit &staged) { return PublishTable(staged); });
	}
	plan.OnTablesUpdated();
}

//! The categorized error of a failed commit request, raising the HTTPException CommitResult::Throw raises
static IcebergCatalogError CommitError(IcebergRestBackend &backend, const CommitResult &result) {
	ErrorData error;
	try {
		result.Throw(backend.GetBaseUrl().GetURLEncoded());
	} catch (std::exception &ex) {
		error = ErrorData(ex);
	}
	return IcebergCatalogError(IcebergRestBackend::MapHTTPStatus(result.status), std::move(error),
	                           result.error_ ? result.error_->_error.type : string());
}

optional<IcebergCatalogError> IcebergRestCommit::PublishTransaction(IcebergStagedCommit &staged) {
	rest_api_objects::CommitTransactionRequest request;
	for (auto &change : staged.changes) {
		request.table_changes.push_back(std::move(change));
	}
	auto result = IRCAPI::CommitMultiTableUpdate(context, backend, RESTObjectToJSONString(request));
	if (result.Success()) {
		return nullopt;
	}
	return CommitError(backend, result);
}

optional<IcebergCatalogError> IcebergRestCommit::PublishTable(IcebergStagedCommit &staged) {
	D_ASSERT(staged.changes.size() == 1);
	auto &table_change = staged.changes[0];
	D_ASSERT(table_change.identifier);
	auto &identifier = *table_change.identifier;
	auto result = IRCAPI::CommitTableUpdate(context, backend, identifier._namespace.value, identifier.name,
	                                        RESTObjectToJSONString(table_change));
	if (result.Success()) {
		return nullopt;
	}
	return CommitError(backend, result);
}

//===--------------------------------------------------------------------===//
// Views
//===--------------------------------------------------------------------===//

void IcebergRestCommit::DropViews() {
	for (auto &view : plan.DroppedViews()) {
		IRCAPI::CommitViewDelete(context, backend, view.ns.levels, view.name);
	}
	plan.OnViewsDropped();
}

//! The CreateViewRequest of a new view
static string CreateViewRequestJSON(const IcebergNewView &view) {
	JSONWriter writer;
	auto root_object = writer.CreateObject();
	writer.SetRoot(root_object);

	// name
	root_object.AddString("name", view.id.name.c_str());

	// schema — the view's output column types
	auto schema_obj = writer.CreateObject();
	root_object.Add("schema", schema_obj);
	schema_obj.AddString("type", "struct");
	schema_obj.Add("schema-id", writer.CreateSignedInteger(0));
	auto fields_arr = writer.CreateArray();
	schema_obj.Add("fields", fields_arr);
	idx_t next_field_id = 1;
	for (idx_t i = 0; i < view.column_types.size(); i++) {
		auto field = IcebergTypeHelper::CreateIcebergRestType(
		    view.column_names[i], view.column_types[i], false, "", Value(),
		    [&next_field_id]() { return next_field_id++; }, 2);
		fields_arr.Append(field.ToJSON(writer));
	}
	auto identifier_field_ids = writer.CreateArray();
	schema_obj.Add("identifier-field-ids", identifier_field_ids);
	(void)identifier_field_ids;

	// view-version
	auto version_obj = writer.CreateObject();
	root_object.Add("view-version", version_obj);
	version_obj.Add("version-id", writer.CreateSignedInteger(1));
	version_obj.Add("timestamp-ms",
	                writer.CreateUnsignedInteger(Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp())));
	version_obj.Add("schema-id", writer.CreateSignedInteger(0));

	// summary
	auto summary_obj = writer.CreateObject();
	version_obj.Add("summary", summary_obj);
	summary_obj.AddString("engine-name", "DuckDB");

	// default-namespace
	auto ns_arr = writer.CreateArray();
	version_obj.Add("default-namespace", ns_arr);
	for (auto &ns_item : view.default_namespace.levels) {
		ns_arr.AppendString(ns_item);
	}

	// Omit default-catalog: unqualified references belong to this REST catalog,
	// even when another session attaches it using a different local name.

	// representations
	auto repr_arr = writer.CreateArray();
	version_obj.Add("representations", repr_arr);
	auto repr_obj = writer.CreateObject();
	repr_arr.Append(repr_obj);
	repr_obj.AddString("type", IcebergConstants::ViewSQLRepresentationType);
	repr_obj.AddString("sql", view.sql.c_str());
	repr_obj.AddString("dialect", IcebergConstants::ViewDuckDBDialect);

	// properties (empty)
	root_object.Add("properties", writer.CreateObject());

	return writer.ToString();
}

void IcebergRestCommit::CreateViews() {
	auto views = plan.CreatedViews(context);
	if (views.empty()) {
		return;
	}
	for (auto &view : views) {
		IRCAPI::CommitNewView(context, backend, view.id.ns.levels, CreateViewRequestJSON(view));
	}
	plan.OnViewsCreated();
}

//===--------------------------------------------------------------------===//
// Outcome and abort
//===--------------------------------------------------------------------===//

// 4xx = definitive rejection (commit did not land) -> delete the orphan. 5xx / no HTTP status
// (connection error/timeout) = outcome unknown (may have landed) -> keep files. Status is in
// extra_info (HTTPException) or the message "status code (<status>)" (fallback).
bool IcebergRestCommit::CommitOutcomeUnknown(const ErrorData &error) {
	auto &extra = error.ExtraInfo();
	auto it = extra.find("status_code");
	if (it != extra.end()) {
		return it->second.empty() || it->second[0] != '4';
	}
	auto &msg = error.RawMessage();
	auto pos = msg.find("status code (");
	if (pos != string::npos) {
		auto digit = msg.find_first_of("0123456789", pos);
		if (digit != string::npos) {
			return msg[digit] != '4';
		}
	}
	return true;
}

//! Whether the catalog still holds the table version that the create request returned.
static bool IsCreatedVersion(const rest_api_objects::LoadTableResult &current,
                             const rest_api_objects::LoadTableResult &created) {
	if (!current.metadata_location || !created.metadata_location) {
		//! Without a metadata location, a commit by another transaction cannot be ruled out.
		return false;
	}
	if (*current.metadata_location != *created.metadata_location) {
		return false;
	}
	return current.metadata.table_uuid == created.metadata.table_uuid;
}

void IcebergRestCommit::DropCreatedTables(IcebergRestBackend &backend, ClientContext &context,
                                          IcebergCommitPlan &plan) {
	for (auto &table : plan.CreatedTables()) {
		auto table_name = IcebergLogType::Redact(context, table.table_key);
		try {
			//! The table has been visible to other transactions since it was created, so it is dropped only while
			//! the catalog still holds the version that the create request returned.
			auto current = IcebergLoadTableRequest(table.id.ns.levels, table.id.name).Execute(context, backend);
			if (current.error_) {
				DUCKDB_LOG(context, IcebergLogType,
				           "Iceberg Transaction Cleanup, did not drop table '%s': loading it returned %s", table_name,
				           EnumUtil::ToString(current.status_));
				continue;
			}
			auto &created = table.created->Cast<IcebergLoadTableResponse>();
			if (!IsCreatedVersion(current.result_->result, created.result)) {
				DUCKDB_LOG(context, IcebergLogType,
				           "Iceberg Transaction Cleanup, did not drop table '%s': the catalog does not report the "
				           "version this transaction created",
				           table_name);
				continue;
			}
			IRCAPI::CommitTableDelete(context, backend, table.id.ns.levels, table.id.name);
			DUCKDB_LOG(context, IcebergLogType, "Iceberg Transaction Cleanup, dropped table '%s'", table_name);
		} catch (std::exception &ex) {
			DUCKDB_LOG(context, IcebergLogType, "Iceberg Transaction Cleanup, failed to drop table '%s': %s",
			           table_name, ex.what());
		}
	}
}

} // namespace duckdb
