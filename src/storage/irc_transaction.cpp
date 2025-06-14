#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "manifest_reader.hpp"

#include "storage/irc_transaction.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_authorization.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

IRCTransaction::IRCTransaction(IRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), db(*context.db), catalog(ic_catalog), access_mode(ic_catalog.access_mode),
      schemas(ic_catalog) {
}

IRCTransaction::~IRCTransaction() = default;

void IRCTransaction::MarkTableAsDirty(const ICTableEntry &table) {
	dirty_tables.insert(&table);
}

void IRCTransaction::Start() {
}

void CommitTableToJSON(yyjson_mut_doc *doc, yyjson_mut_val *root_object,
                       const rest_api_objects::CommitTableRequest &table) {
	//! requirements
	auto requirements_array = yyjson_mut_obj_add_arr(doc, root_object, "requirements");
	D_ASSERT(table.requirements.empty());

	//! updates
	auto updates_array = yyjson_mut_obj_add_arr(doc, root_object, "updates");
	for (auto &update : table.updates) {
		if (update.has_add_snapshot_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", "add-snapshot");
			//! updates[...].snapshot
			auto snapshot_json = yyjson_mut_obj_add_obj(doc, update_json, "snapshot");

			auto &snapshot = update.add_snapshot_update.snapshot;
			yyjson_mut_obj_add_uint(doc, snapshot_json, "snapshot-id", snapshot.snapshot_id);
			if (snapshot.has_parent_snapshot_id) {
				yyjson_mut_obj_add_uint(doc, snapshot_json, "parent-snapshot-id", snapshot.parent_snapshot_id);
			}
			yyjson_mut_obj_add_uint(doc, snapshot_json, "sequence-number", snapshot.sequence_number);
			yyjson_mut_obj_add_uint(doc, snapshot_json, "timestamp-ms", snapshot.timestamp_ms);
			yyjson_mut_obj_add_strcpy(doc, snapshot_json, "manifest-list", snapshot.manifest_list.c_str());
			auto summary_json = yyjson_mut_obj_add_obj(doc, snapshot_json, "summary");
			yyjson_mut_obj_add_strcpy(doc, summary_json, "operation", snapshot.summary.operation.c_str());
			yyjson_mut_obj_add_uint(doc, snapshot_json, "schema-id", snapshot.schema_id);
		} else if (update.has_set_snapshot_ref_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.set_snapshot_ref_update;

			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", "set-snapshot-ref");
			//! updates[...].ref-name
			yyjson_mut_obj_add_strcpy(doc, update_json, "ref-name", ref_update.ref_name.c_str());
			//! updates[...].type
			yyjson_mut_obj_add_strcpy(doc, update_json, "type", ref_update.snapshot_reference.type.c_str());
			//! updates[...].snapshot-id
			yyjson_mut_obj_add_uint(doc, update_json, "snapshot-id", ref_update.snapshot_reference.snapshot_id);
		} else {
			throw NotImplementedException("Can't serialize this TableUpdate type to JSON");
		}
	}

	//! identifier
	D_ASSERT(table.has_identifier);
	auto &_namespace = table.identifier._namespace.value;
	auto identifier_json = yyjson_mut_obj_add_obj(doc, root_object, "identifier");

	//! identifier.name
	yyjson_mut_obj_add_strcpy(doc, identifier_json, "name", table.identifier.name.c_str());
	//! identifier.namespace
	auto namespace_arr = yyjson_mut_obj_add_arr(doc, identifier_json, "namespace");
	D_ASSERT(_namespace.size() == 1);
	yyjson_mut_arr_add_strcpy(doc, namespace_arr, _namespace[0].c_str());
}

void CommitTransactionToJSON(yyjson_mut_doc *doc, yyjson_mut_val *root_object,
                             const rest_api_objects::CommitTransactionRequest &req) {
	auto table_changes_array = yyjson_mut_obj_add_arr(doc, root_object, "table-changes");
	for (auto &table : req.table_changes) {
		auto table_obj = yyjson_mut_arr_add_obj(doc, table_changes_array);
		CommitTableToJSON(doc, table_obj, table);
	}
}

string JsonDocToString(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc) {
	auto root_object = yyjson_mut_doc_get_root(doc.get());

	//! Write the result to a string
	auto data = yyjson_mut_val_write_opts(root_object, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, nullptr, nullptr);
	if (!data) {
		throw InvalidInputException("Could not create a JSON representation of the table schema, yyjson failed");
	}
	auto res = string(data);
	free(data);
	return res;
}

void IRCTransaction::Commit() {
	if (dirty_tables.empty()) {
		return;
	}

	Connection temp_con(db);
	auto &context = temp_con.context;
	context->transaction.BeginTransaction();

	rest_api_objects::CommitTransactionRequest transaction;
	for (auto &table : dirty_tables) {
		IcebergCommitState commit_state;
		auto &table_change = commit_state.table_change;
		table_change.identifier._namespace.value.push_back(table->ParentSchema().name);
		table_change.identifier.name = table->name;
		table_change.has_identifier = true;

		auto &metadata = table->table_info.table_metadata;
		auto current_snapshot = metadata.GetLatestSnapshot();
		if (current_snapshot) {
			auto &manifest_list_path = current_snapshot->manifest_list;
			//! Read the manifest list
			auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(metadata.iceberg_version);
			auto scan = make_uniq<AvroScan>("IcebergManifestList", *context, manifest_list_path);
			manifest_list_reader->Initialize(std::move(scan));
			while (!manifest_list_reader->Finished()) {
				manifest_list_reader->Read(STANDARD_VECTOR_SIZE, commit_state.manifests);
			}
		}

		auto &transaction_data = *table->table_info.transaction_data;
		for (auto &update : transaction_data.updates) {
			update->CreateUpdate(db, *context, commit_state);
		}
		transaction.table_changes.push_back(std::move(table_change));
	}

	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);

	CommitTransactionToJSON(doc, root_object, transaction);
	auto transaction_json = JsonDocToString(std::move(doc_p));

	auto &authentication = *catalog.auth_handler;
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("transactions");
	url_builder.AddPathComponent("commit");

	auto response = authentication.PostRequest(*context, url_builder, transaction_json);
	if (response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
		    EnumUtil::ToString(response->status), response->reason, response->body);
	}
	context->transaction.Commit();
}

void IRCTransaction::CleanupFiles() {
	// remove any files that were written
	auto &fs = FileSystem::GetFileSystem(db);
	for (auto &table : dirty_tables) {
		auto &transaction_data = *table->table_info.transaction_data;
		for (auto &update : transaction_data.updates) {
			if (update->type != IcebergTableUpdateType::ADD_SNAPSHOT) {
				continue;
			}
			auto &add_snapshot = update->Cast<IcebergAddSnapshot>();
			auto &data_files = add_snapshot.manifest_file.data_files;
			for (auto &data_file : data_files) {
				fs.TryRemoveFile(data_file.file_path);
			}
		}
	}
}

void IRCTransaction::Rollback() {
	CleanupFiles();
}

IRCTransaction &IRCTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<IRCTransaction>();
}

} // namespace duckdb
