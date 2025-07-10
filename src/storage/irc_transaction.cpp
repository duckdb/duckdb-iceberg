#include "duckdb/common/assert.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "manifest_reader.hpp"

#include "storage/irc_transaction.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_authorization.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/table_create/iceberg_create_table_request.hpp"
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

void IRCTransaction::MarkTableAsNew(const ICTableEntry &table) {
	new_tables.insert(&table);
}

void IRCTransaction::Start() {
}

IRCatalog &IRCTransaction::GetCatalog() {
	return catalog;
}

void CommitTableToJSON(yyjson_mut_doc *doc, yyjson_mut_val *root_object,
                       const rest_api_objects::CommitTableRequest &table) {
	//! requirements
	auto requirements_array = yyjson_mut_obj_add_arr(doc, root_object, "requirements");
	for (auto &requirement : table.requirements) {
		if (requirement.has_assert_ref_snapshot_id) {
			auto &assert_ref_snapshot_id = requirement.assert_ref_snapshot_id;
			auto requirement_json = yyjson_mut_arr_add_obj(doc, requirements_array);
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "type", assert_ref_snapshot_id.type.value.c_str());
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "ref", assert_ref_snapshot_id.ref.c_str());
			yyjson_mut_obj_add_uint(doc, requirement_json, "snapshot-id", assert_ref_snapshot_id.snapshot_id);
		} else {
			throw NotImplementedException("Can't serialize this TableRequirement type to JSON");
		}
	}

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

static string ConstructNamespace(vector<string> namespaces) {
	auto table_namespace = std::accumulate(namespaces.begin() + 1, namespaces.end(), namespaces[0],
	                                       [](const std::string &a, const std::string &b) { return a + "." + b; });
	return table_namespace;
}

static string ConstructTableUpdateJSON(rest_api_objects::CommitTableRequest &table_change) {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);
	CommitTableToJSON(doc, root_object, table_change);
	return JsonDocToString(std::move(doc_p));
}

static rest_api_objects::TableRequirement CreateAssertRefSnapshotIdRequirement(IcebergSnapshot &old_snapshot) {
	rest_api_objects::TableRequirement req;
	req.has_assert_ref_snapshot_id = true;

	auto &res = req.assert_ref_snapshot_id;
	res.ref = "main";
	res.snapshot_id = old_snapshot.snapshot_id;
	res.type.value = "assert-ref-snapshot-id";
	return req;
}

rest_api_objects::LoadTableResult IRCTransaction::CommitNewTable(ClientContext &context, const ICTableEntry *table,
                                                                 bool stage_create) {
	auto &ic_catalog = table->catalog.Cast<IRCatalog>();
	// Stupid hack for GLUE catalog
	if (!stage_create && !ic_catalog.attach_options.supports_stage_create) {
		// if the catalog does not support stage create, and stage_create = false
		// it means we are trying to commit the table. We have already hit the endpoint with stage_create = true
		// so this second request will result in an error. so we early out
		return rest_api_objects::LoadTableResult();
	}
	// TODO: add D_ASSERT for the post table url
	auto table_namespace = table->schema.name;
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(table_namespace);
	url_builder.AddPathComponent("tables");

	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	yyjson_mut_doc *doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);

	auto initial_schema = table->table_info.table_metadata.schemas[table->table_info.table_metadata.current_schema_id];
	auto create_transaction = make_uniq<IcebergCreateTableRequest>(initial_schema, table->table_info.name);
	if (stage_create && ic_catalog.attach_options.supports_stage_create) {
		yyjson_mut_obj_add_bool(doc, root_object, "stage-create", stage_create);
	} else {
		yyjson_mut_obj_add_bool(doc, root_object, "stage-create", false);
	}
	auto create_table_json = create_transaction->CreateTableToJSON(std::move(doc_p));

	try {
		auto response = catalog.auth_handler->PostRequest(context, url_builder, create_table_json);
		if (response->status != HTTPStatusCode::OK_200) {
			throw InvalidConfigurationException(
			    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
			    EnumUtil::ToString(response->status), response->reason, response->body);
		}
		std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
		auto *root = yyjson_doc_get_root(doc.get());
		auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(root);
		return load_table_result;
	} catch (const std::exception &e) {
		throw InvalidConfigurationException("Request to '%s' returned a non-200 status code body: %s",
		                                    url_builder.GetURL(), e.what());
	}
}

void IRCTransaction::DropSecrets(ClientContext &context) {
	auto &secret_manager = SecretManager::Get(context);
	for (auto &secret_name : created_secrets) {
		(void)secret_manager.DropSecretByName(context, secret_name, OnEntryNotFound::RETURN_NULL);
	}
}

rest_api_objects::CommitTransactionRequest IRCTransaction::GetTransactionRequest(ClientContext &context) {
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
			auto scan = make_uniq<AvroScan>("IcebergManifestList", context, manifest_list_path);
			manifest_list_reader->Initialize(std::move(scan));
			while (!manifest_list_reader->Finished()) {
				manifest_list_reader->Read(STANDARD_VECTOR_SIZE, commit_state.manifests);
			}
		}

		auto &transaction_data = *table->table_info.transaction_data;
		for (auto &update : transaction_data.updates) {
			update->CreateUpdate(db, context, commit_state);
		}

		if (!transaction_data.alters.empty()) {
			if (current_snapshot) {
				//! If any changes were made to the data of the table, we should assert that our parent snapshot has
				//! not changed
				commit_state.table_change.requirements.push_back(
				    CreateAssertRefSnapshotIdRequirement(*current_snapshot));
			}

			auto &last_alter = transaction_data.alters.back();
			commit_state.table_change.updates.push_back(last_alter.get().CreateSetSnapshotRefUpdate());
		}

		transaction.table_changes.push_back(std::move(table_change));
	}
	return transaction;
}

void IRCTransaction::Commit() {

	if (dirty_tables.empty() && new_tables.empty()) {
		return;
	}

	Connection temp_con(db);
	temp_con.BeginTransaction();
	auto &context = temp_con.context;

	if (!new_tables.empty()) {
		for (auto &table : new_tables) {
			// we need to reload the secrets again because we are working in a different
			// transaction with a different context again.
			table->PrepareIcebergScanFromEntry(*context);
			if (table->table_info.transaction_data->create) {
				CommitNewTable(*context, table);
			}
		}
	}

	if (dirty_tables.empty()) {
		// we don't need to do anything here.
		temp_con.Rollback();
		return;
	}

	try {
		auto transaction = GetTransactionRequest(*context);
		auto &authentication = *catalog.auth_handler;
		if (catalog.supported_urls.find("POST /v1/{prefix}/transactions/commit") != catalog.supported_urls.end()) {
			// commit all transactions at once
			std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
			auto doc = doc_p.get();
			auto root_object = yyjson_mut_obj(doc);
			yyjson_mut_doc_set_root(doc, root_object);

			CommitTransactionToJSON(doc, root_object, transaction);
			auto transaction_json = JsonDocToString(std::move(doc_p));
			auto url_builder = catalog.GetBaseUrl();
			url_builder.AddPathComponent(catalog.prefix);
			url_builder.AddPathComponent("transactions");
			url_builder.AddPathComponent("commit");

			auto response = authentication.PostRequest(*context, url_builder, transaction_json);
			if (response->status != HTTPStatusCode::OK_200) {
				throw InvalidConfigurationException(
				    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
				    url_builder.GetURL(), EnumUtil::ToString(response->status), response->reason, response->body);
			}
		} else {
			D_ASSERT(catalog.supported_urls.find("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}") !=
			         catalog.supported_urls.end());
			// each table change will make a separate request
			for (auto &table_change : transaction.table_changes) {
				D_ASSERT(table_change.has_identifier);

				auto table_namespace = ConstructNamespace(table_change.identifier._namespace.value);
				auto url_builder = catalog.GetBaseUrl();
				url_builder.AddPathComponent(catalog.prefix);
				url_builder.AddPathComponent("namespaces");
				url_builder.AddPathComponent(table_namespace);
				url_builder.AddPathComponent("tables");
				url_builder.AddPathComponent(table_change.identifier.name);

				auto transaction_json = ConstructTableUpdateJSON(table_change);

				auto response = authentication.PostRequest(*context, url_builder, transaction_json);
				if (response->status != HTTPStatusCode::OK_200) {
					throw InvalidConfigurationException(
					    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
					    url_builder.GetURL(), EnumUtil::ToString(response->status), response->reason, response->body);
				}
			}
		}
		DropSecrets(*context);
	} catch (std::exception &ex) {
		if (!new_tables.empty()) {
			// hit drop enpoints.
		}
		ErrorData error(ex);
		CleanupFiles();
		DropSecrets(*context);
		temp_con.Rollback();
		error.Throw("Failed to commit Iceberg transaction: ");
	}

	temp_con.Rollback();
}

void IRCTransaction::CleanupFiles() {
	// remove any files that were written
	if (!catalog.attach_options.allows_deletes) {
		// certain catalogs don't allow deletes and will have a s3.deletes attribute in the config describing this
		// aws s3 tables rejects deletes and will handle garbage collection on its own, any attempt to delete the files
		// on the aws side will result in an error.
		return;
	}
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
