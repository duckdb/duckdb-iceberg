#include "catalog/rest/transaction/iceberg_transaction.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/main/client_data.hpp"
#include "yyjson.hpp"

#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/api/iceberg_add_snapshot.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "../../../include/catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"

namespace duckdb {

IcebergTransactionTableState::IcebergTransactionTableState(optional_ptr<IcebergTableInformation> table)
    : table(table), status(table ? IcebergTableStatus::ALIVE : IcebergTableStatus::MISSING) {
}

IcebergTransaction::IcebergTransaction(IcebergCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), db(*context.db), catalog(ic_catalog), access_mode(ic_catalog.access_mode) {
}

IcebergTransaction::~IcebergTransaction() = default;

void IcebergTransaction::Start() {
}

IcebergCatalog &IcebergTransaction::GetCatalog() {
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
			if (assert_ref_snapshot_id.has_snapshot_id) {
				yyjson_mut_obj_add_uint(doc, requirement_json, "snapshot-id", assert_ref_snapshot_id.snapshot_id);
			} else {
				yyjson_mut_obj_add_null(doc, requirement_json, "snapshot-id");
			}
		} else if (requirement.has_assert_create) {
			auto &assert_create = requirement.assert_create;
			auto requirement_json = yyjson_mut_arr_add_obj(doc, requirements_array);
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "type", assert_create.type.value.c_str());
		} else if (requirement.has_assert_current_schema_id) {
			auto &assert_current_schema_id = requirement.assert_current_schema_id;
			auto requirement_json = yyjson_mut_arr_add_obj(doc, requirements_array);
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "type", assert_current_schema_id.type.value.c_str());
			yyjson_mut_obj_add_int(doc, requirement_json, "current-schema-id",
			                       assert_current_schema_id.current_schema_id);
		} else if (requirement.has_assert_last_assigned_field_id) {
			auto &assert_last_assigned_field_id = requirement.assert_last_assigned_field_id;
			auto requirement_json = yyjson_mut_arr_add_obj(doc, requirements_array);
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "type", assert_last_assigned_field_id.type.value.c_str());
			yyjson_mut_obj_add_int(doc, requirement_json, "last-assigned-field-id",
			                       assert_last_assigned_field_id.last_assigned_field_id);
		} else if (requirement.has_assert_last_assigned_partition_id) {
			auto &assert_last_assigned_partition_id = requirement.assert_last_assigned_partition_id;
			auto requirement_json = yyjson_mut_arr_add_obj(doc, requirements_array);
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "type",
			                          assert_last_assigned_partition_id.type.value.c_str());
			yyjson_mut_obj_add_int(doc, requirement_json, "last-assigned-partition-id",
			                       assert_last_assigned_partition_id.last_assigned_partition_id);
		} else if (requirement.has_assert_default_spec_id) {
			auto &assert_default_spec_id = requirement.assert_default_spec_id;
			auto requirement_json = yyjson_mut_arr_add_obj(doc, requirements_array);
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "type", assert_default_spec_id.type.value.c_str());
			yyjson_mut_obj_add_int(doc, requirement_json, "default-spec-id", assert_default_spec_id.default_spec_id);
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
			auto &snapshot = update.add_snapshot_update.snapshot;
			auto snapshot_obj = IcebergSnapshot::ToJSON(snapshot, doc);
			yyjson_mut_obj_add_val(doc, update_json, "snapshot", snapshot_obj);
		} else if (update.has_set_snapshot_ref_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.set_snapshot_ref_update;

			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			//! updates[...].ref-name
			yyjson_mut_obj_add_strcpy(doc, update_json, "ref-name", ref_update.ref_name.c_str());
			//! updates[...].type
			yyjson_mut_obj_add_strcpy(doc, update_json, "type", ref_update.snapshot_reference.type.c_str());
			//! updates[...].snapshot-id
			yyjson_mut_obj_add_uint(doc, update_json, "snapshot-id", ref_update.snapshot_reference.snapshot_id);
		} else if (update.has_assign_uuidupdate) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.assign_uuidupdate;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			//! updates[...].ref-name
			yyjson_mut_obj_add_strcpy(doc, update_json, "uuid", ref_update.uuid.c_str());
		} else if (update.has_upgrade_format_version_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.upgrade_format_version_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			//! updates[...].ref-name
			yyjson_mut_obj_add_uint(doc, update_json, "format-version", ref_update.format_version);
		} else if (update.has_set_properties_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.set_properties_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			auto properties_json = yyjson_mut_obj_add_obj(doc, update_json, "updates");
			for (auto &prop : ref_update.updates) {
				yyjson_mut_obj_add_strcpy(doc, properties_json, prop.first.c_str(), prop.second.c_str());
			}
		} else if (update.has_remove_properties_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.remove_properties_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			auto properties_json = yyjson_mut_obj_add_arr(doc, update_json, "removals");
			for (auto &prop : ref_update.removals) {
				yyjson_mut_arr_add_strcpy(doc, properties_json, prop.c_str());
			}
		} else if (update.has_add_schema_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.add_schema_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			yyjson_mut_obj_add_uint(doc, update_json, "last-column-id", update.add_schema_update.last_column_id);
			auto schema_json = yyjson_mut_obj_add_obj(doc, update_json, "schema");
			IcebergTableSchema::SchemaToJson(doc, schema_json, update.add_schema_update.schema);
		} else if (update.has_set_current_schema_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.set_current_schema_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			yyjson_mut_obj_add_int(doc, update_json, "schema-id", ref_update.schema_id);
		} else if (update.has_set_default_spec_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.set_default_spec_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			yyjson_mut_obj_add_int(doc, update_json, "spec-id", ref_update.spec_id);
		} else if (update.has_add_partition_spec_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.add_partition_spec_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			yyjson_mut_obj_add_val(doc, update_json, "spec", IcebergPartitionSpec::ToJSON(doc, ref_update.spec));
		} else if (update.has_set_default_sort_order_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.set_default_sort_order_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			yyjson_mut_obj_add_int(doc, update_json, "sort-order-id", ref_update.sort_order_id);
		} else if (update.has_add_sort_order_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.add_sort_order_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			auto sort_order_json = yyjson_mut_obj_add_obj(doc, update_json, "sort-order");
			yyjson_mut_obj_add_int(doc, sort_order_json, "order-id", ref_update.sort_order.order_id);
			// Add fields array, later we can add the fields
			auto fields_arr = yyjson_mut_obj_add_arr(doc, sort_order_json, "fields");
			(void)fields_arr;
		} else if (update.has_set_location_update) {
			auto update_json = yyjson_mut_arr_add_obj(doc, updates_array);
			auto &ref_update = update.set_location_update;
			//! updates[...].action
			yyjson_mut_obj_add_strcpy(doc, update_json, "action", ref_update.action.c_str());
			yyjson_mut_obj_add_strcpy(doc, update_json, "location", ref_update.location.c_str());
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
	D_ASSERT(_namespace.size() >= 1);
	for (auto &identifier : _namespace) {
		yyjson_mut_arr_add_strcpy(doc, namespace_arr, identifier.c_str());
	}
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

static string ConstructTableUpdateJSON(rest_api_objects::CommitTableRequest &table_change) {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);
	CommitTableToJSON(doc, root_object, table_change);
	return JsonDocToString(std::move(doc_p));
}

static rest_api_objects::TableRequirement CreateAssertRefSnapshotIdRequirement(const IcebergSnapshot &old_snapshot) {
	rest_api_objects::TableRequirement req;
	req.has_assert_ref_snapshot_id = true;

	auto &res = req.assert_ref_snapshot_id;
	res.ref = "main";
	res.snapshot_id = old_snapshot.snapshot_id;
	res.has_snapshot_id = true;
	res.type.value = "assert-ref-snapshot-id";
	return req;
}

static rest_api_objects::TableRequirement CreateAssertNoSnapshotRequirement() {
	rest_api_objects::TableRequirement req;
	req.has_assert_ref_snapshot_id = true;

	auto &res = req.assert_ref_snapshot_id;
	res.ref = "main";
	res.has_snapshot_id = false;
	res.type.value = "assert-ref-snapshot-id";
	return req;
}

void IcebergTransaction::DropSecrets(ClientContext &context) {
	auto &secret_manager = SecretManager::Get(context);
	for (auto &secret_name : created_secrets) {
		(void)secret_manager.DropSecretByName(context, secret_name, OnEntryNotFound::RETURN_NULL);
	}
}

static rest_api_objects::TableUpdate CreateSetSnapshotRefUpdate(int64_t snapshot_id) {
	rest_api_objects::TableUpdate table_update;

	table_update.has_set_snapshot_ref_update = true;
	auto &update = table_update.set_snapshot_ref_update;
	update.base_update.action = "set-snapshot-ref";
	update.has_action = true;
	update.action = "set-snapshot-ref";

	update.ref_name = "main";
	update.snapshot_reference.type = "branch";
	update.snapshot_reference.snapshot_id = snapshot_id;
	return table_update;
}

static bool NeedsAssertSchemaId(const IcebergTransactionData &transaction_data,
                                const IcebergTableInformation &table_info) {
	if (!transaction_data.assert_schema_id) {
		return false;
	}
	auto &initial_schema_id = transaction_data.initial_schema_id;
	return initial_schema_id != table_info.table_metadata.GetCurrentSchemaId();
}

TableTransactionInfo IcebergTransaction::GetTransactionRequest(IcebergTransactionAlterUpdate &alter_update,
                                                               ClientContext &context) {
	TableTransactionInfo info;
	auto &transaction = info.request;
	for (auto &updated_table : alter_update.updated_tables) {
		if (alter_update.committed_tables.count(updated_table.first)) {
			//! Table is already committed
			continue;
		}
		auto &table_info = updated_table.second;
		if (!table_info.transaction_data) {
			continue;
		}
		IcebergCommitState commit_state(table_info, context);
		auto &table_change = commit_state.table_change;
		auto &schema = table_info.schema.Cast<IcebergSchemaEntry>();
		table_change.identifier._namespace.value = schema.namespace_items;
		table_change.identifier.name = table_info.name;
		table_change.has_identifier = true;

		auto &metadata = commit_state.table_info.table_metadata;
		auto current_snapshot = metadata.GetLatestSnapshot();
		auto &transaction_data = *commit_state.table_info.transaction_data;
		if (!transaction_data.alters.empty()) {
			commit_state.manifests = transaction_data.existing_manifest_list;
		}
		commit_state.latest_snapshot = current_snapshot;

		for (auto &update : transaction_data.updates) {
			if (update->type == IcebergTableUpdateType::ADD_SNAPSHOT) {
				// we need to recreate the keys in the current context.
				auto &ic_table_entry = table_info.GetLatestSchema(context)->Cast<IcebergTableEntry>();
				ic_table_entry.PrepareIcebergScanFromEntry(context);
			}
			update->CreateUpdate(db, context, commit_state);
		}
		for (auto &requirement : transaction_data.requirements) {
			requirement->CreateRequirement(db, context, commit_state);
			info.has_assert_create = requirement->type == IcebergTableRequirementType::ASSERT_CREATE;
		}
		if (!info.has_assert_create && NeedsAssertSchemaId(transaction_data, table_info)) {
			// Ensure schema is the same as current
			AssertCurrentSchemaIdRequirement requirement(table_info);
			requirement.current_schema_id = transaction_data.initial_schema_id;
			requirement.CreateRequirement(db, context, commit_state);
		}

		if (!transaction_data.alters.empty()) {
			auto &snapshot = *commit_state.latest_snapshot;
			auto snapshot_id = snapshot.snapshot_id;
			auto set_snapshot_ref_update = CreateSetSnapshotRefUpdate(snapshot_id);
			commit_state.table_change.updates.push_back(std::move(set_snapshot_ref_update));
		}

		if (current_snapshot && !transaction_data.alters.empty()) {
			//! If any changes were made to the state of the table, we should assert that our parent snapshot has
			//! not changed. We don't want to change the table location if someone has added a snapshot
			commit_state.table_change.requirements.push_back(CreateAssertRefSnapshotIdRequirement(*current_snapshot));
		} else if (!current_snapshot && !transaction_data.alters.empty() && !info.has_assert_create) {
			//! If the table had no snapshots, is not created in this transaction, and has some kind of update
			//! we should ensure no snapshots have been added in the meantime
			commit_state.table_change.requirements.push_back(CreateAssertNoSnapshotRequirement());
		}

		if (transaction_data.set_schema_id) {
			SetCurrentSchema update(table_info);
			update.CreateUpdate(db, context, commit_state);
		}

		info.table_requests.emplace(updated_table.first, transaction.table_changes.size());
		transaction.table_changes.push_back(std::move(table_change));
	}
	return info;
}

void IcebergTransaction::Commit() {
	if (transaction_updates.empty() && created_schemas.empty() && deleted_schemas.empty() &&
	    schema_property_updates.empty()) {
		return;
	}

	Connection temp_con(db);
	temp_con.BeginTransaction();
	auto &temp_con_context = temp_con.context;

	// Copy user settings from the original context so that e.g. s3_access_key_id are available
	if (!this->context.expired()) {
		temp_con_context->config = this->context.lock()->config;
	}

	try {
		DoSchemaCreates(*temp_con_context);
		DoSchemaPropertyUpdates(*temp_con_context);
		for (auto &transaction_update : transaction_updates) {
			auto &type = transaction_update->type;
			switch (type) {
			case IcebergTransactionUpdateType::ALTER: {
				auto &alter_update = transaction_update->Cast<IcebergTransactionAlterUpdate>();
				DoTableUpdates(alter_update, *temp_con_context);
				break;
			}
			case IcebergTransactionUpdateType::DELETE: {
				auto &delete_update = transaction_update->Cast<IcebergTransactionDeleteUpdate>();
				DoTableDeletes(delete_update, *temp_con_context);
				break;
			}
			case IcebergTransactionUpdateType::RENAME: {
				auto &rename_update = transaction_update->Cast<IcebergTransactionRenameUpdate>();
				DoTableRename(rename_update, *temp_con_context);
				break;
			}
			default:
				throw InternalException("IcebergTransactionUpdateType (%d) not implemented",
				                        static_cast<uint8_t>(type));
			};
		}
		DoSchemaDeletes(*temp_con_context);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		CleanupFiles();
		DropSecrets(*temp_con_context);
		temp_con.Rollback();
		error.Throw("Failed to commit Iceberg transaction: ");
	}

	temp_con.Rollback();
}

void IcebergTransaction::DoTableUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context) {
	if (!alter_update.HasUpdates()) {
		return;
	}
	auto transaction_info = GetTransactionRequest(alter_update, context);
	auto &transaction = transaction_info.request;

	// if there are no new tables, we can post to the transactions/commit endpoint
	// otherwise we fall back to posting a commit for each table.
	const bool can_use_multi_table_commit =
	    !transaction_info.has_assert_create && catalog.supported_urls.count("POST /v1/{prefix}/transactions/commit");
	if (can_use_multi_table_commit) {
		// commit all transactions at once
		std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
		auto doc = doc_p.get();
		auto root_object = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, root_object);

		CommitTransactionToJSON(doc, root_object, transaction);
		auto transaction_json = JsonDocToString(std::move(doc_p));
		IRCAPI::CommitMultiTableUpdate(context, catalog, transaction_json);
		for (auto &it : alter_update.updated_tables) {
			alter_update.committed_tables.insert(it.first);
		}
	} else {
		D_ASSERT(catalog.supported_urls.count("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
		// each table change will make a separate request
		for (auto &it : transaction_info.table_requests) {
			auto &table_change = transaction.table_changes[it.second];
			D_ASSERT(table_change.has_identifier);
			auto transaction_json = ConstructTableUpdateJSON(table_change);
			IRCAPI::CommitTableUpdate(context, catalog, table_change.identifier._namespace.value,
			                          table_change.identifier.name, transaction_json);
			alter_update.committed_tables.insert(it.first);
		}
	}
	DropSecrets(context);
}

static yyjson_mut_val *CreateRenameComponentJSON(yyjson_mut_doc *doc, const IcebergSchemaEntry &schema,
                                                 const string &table_name) {
	auto res = yyjson_mut_obj(doc);
	auto namespace_arr = yyjson_mut_arr(doc);
	for (auto &item : schema.namespace_items) {
		yyjson_mut_arr_add_strcpy(doc, namespace_arr, item.c_str());
	}
	yyjson_mut_obj_add_val(doc, res, "namespace", namespace_arr);
	yyjson_mut_obj_add_strcpy(doc, res, "name", table_name.c_str());
	return res;
}

static yyjson_mut_val *CreateRenameRequestJSON(yyjson_mut_doc *doc, const IcebergSchemaEntry &schema,
                                               const string &source, const string &destination) {
	//  value: {
	//    "source": { "namespace": ["accounting", "tax"], "name": "paid" },
	//    "destination": { "namespace": ["accounting", "tax"], "name": "owed" }
	//  }
	auto res = yyjson_mut_obj(doc);

	auto source_obj = CreateRenameComponentJSON(doc, schema, source);
	auto destination_obj = CreateRenameComponentJSON(doc, schema, destination);
	yyjson_mut_obj_add_val(doc, res, "source", source_obj);
	yyjson_mut_obj_add_val(doc, res, "destination", destination_obj);
	return res;
}

void IcebergTransaction::DoTableRename(IcebergTransactionRenameUpdate &rename_update, ClientContext &context) {
	auto &original_table = rename_update.table;
	auto &schema = original_table.schema;
	auto table_key = original_table.GetTableKey();
	auto &table_name = original_table.name;
	auto new_name = rename_update.new_name;

	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root_object = CreateRenameRequestJSON(doc, schema, table_name, new_name);
	yyjson_mut_doc_set_root(doc, root_object);
	auto transaction_json = JsonDocToString(std::move(doc_p));
	IRCAPI::CommitTableRename(context, catalog, transaction_json);

	DropInfo drop_info;
	drop_info.name = table_name;
	drop_info.if_not_found = OnEntryNotFound::THROW_EXCEPTION;
	schema.DropEntry(context, drop_info, true);

	lock_guard<mutex> guard(schema.tables.GetEntryLock());
	shared_ptr<IcebergTableInformation> old_version;
	schema.tables.CreateEntryInternal(guard, new_name, std::move(rename_update.new_table), old_version);
	if (old_version) {
		throw TransactionException("Table %s was already created by a different transaction!", new_name);
	}
}

void IcebergTransaction::DoTableDeletes(IcebergTransactionDeleteUpdate &delete_update, ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &table = delete_update.deleted_table;
	auto schema_key = table.schema.name;
	auto table_key = table.GetTableKey();
	auto &table_name = table.name;
	IRCAPI::CommitTableDelete(context, catalog, table.schema.namespace_items, table_name);
	// remove the load table result
	ic_catalog.table_request_cache.Expire(context, table_key);
	// remove the table entry from the catalog
	auto &schema_entry = ic_catalog.schemas.GetEntry(schema_key).Cast<IcebergSchemaEntry>();
	DropInfo drop_info;
	drop_info.name = table_name;
	drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
	schema_entry.DropEntry(context, drop_info, true);
}

void IcebergTransaction::DoSchemaCreates(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	for (auto &schema_name : created_schemas) {
		auto namespace_identifiers = IRCAPI::ParseSchemaName(schema_name);

		std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
		auto doc = doc_p.get();
		auto root_object = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, root_object);
		auto namespace_arr = yyjson_mut_obj_add_arr(doc, root_object, "namespace");
		for (auto &name : namespace_identifiers) {
			yyjson_mut_arr_add_strcpy(doc, namespace_arr, name.c_str());
		}
		yyjson_mut_obj_add_obj(doc, root_object, "properties");
		auto create_body = JsonDocToString(std::move(doc_p));

		IRCAPI::CommitNamespaceCreate(context, ic_catalog, create_body);
	}
	created_schemas.clear();
}

void IcebergTransaction::DoSchemaDeletes(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	for (auto &schema_name : deleted_schemas) {
		vector<string> namespace_items;
		auto namespace_identifier = IRCAPI::ParseSchemaName(schema_name);
		IRCAPI::CommitNamespaceDrop(context, ic_catalog, namespace_identifier);
		ic_catalog.GetSchemas().RemoveEntry(schema_name);
	}
	deleted_schemas.clear();
}

void IcebergTransaction::DoSchemaPropertyUpdates(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	for (auto &properties_update : this->schema_property_updates) {
		auto schema_name_with_catalog = properties_update.first;
		auto catalog_splitter = schema_name_with_catalog.find(".");
		auto schema_name_no_catalog = schema_name_with_catalog.erase(0, catalog_splitter + 1);

		auto schema_property_updates = properties_update.second;
		auto namespace_identifiers = IRCAPI::ParseSchemaName(schema_name_no_catalog);

		std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
		auto doc = doc_p.get();
		auto root_object = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, root_object);

		auto removal_arr = yyjson_mut_obj_add_arr(doc, root_object, "removals");
		for (auto &removal : schema_property_updates.removals) {
			yyjson_mut_arr_add_strcpy(doc, removal_arr, removal.c_str());
		}
		auto updates_arr = yyjson_mut_obj_add_obj(doc, root_object, "updates");
		for (auto &update : schema_property_updates.updates) {
			yyjson_mut_obj_add_strcpy(doc, updates_arr, update.first.c_str(), update.second.c_str());
		}
		auto create_body = JsonDocToString(std::move(doc_p));

		IRCAPI::CommitNamespacePropertiesUpdate(context, ic_catalog, create_body, schema_name_no_catalog);
	}
	created_schemas.clear();
}

namespace {

struct ScopedTransaction {
public:
	ScopedTransaction(DatabaseInstance &db) : connection(db) {
		connection.BeginTransaction();
	}
	~ScopedTransaction() {
		//! Prevent the connection from destructing with an active transaction
		//! As that causes it to ROLLBACK and enter CleanupFiles - resulting in a stack overflow due to recursion
		connection.Commit();
	}

public:
	ClientContext &GetContext() {
		return *connection.context;
	}

public:
	Connection connection;
};

} // namespace

void IcebergTransaction::CleanupFiles() {
	// remove any files that were written
	if (!catalog.attach_options.allows_deletes) {
		// certain catalogs don't allow deletes and will have a s3.deletes attribute in the config describing this
		// aws s3 tables rejects deletes and will handle garbage collection on its own, any attempt to delete the files
		// on the aws side will result in an error.
		return;
	}
	ScopedTransaction temp_con(db);
	auto &temp_context = temp_con.GetContext();
	auto &fs = FileSystem::GetFileSystem(temp_context);

	for (auto &transaction_update : transaction_updates) {
		if (transaction_update->type != IcebergTransactionUpdateType::ALTER) {
			continue;
		}
		auto &alter_update = transaction_update->Cast<IcebergTransactionAlterUpdate>();
		for (auto &up_table : alter_update.updated_tables) {
			if (alter_update.committed_tables.count(up_table.first)) {
				//! Successively committed, no need to roll back
				continue;
			}
			auto &table = up_table.second;
			if (!table.transaction_data) {
				// error occurred before transaction data was initialized
				// this can happen during table creation with table schema that cannot convert to
				// an iceberg table schema due to type incompatabilities
				continue;
			}
			auto &transaction_data = table.transaction_data;
			for (auto &update : transaction_data->updates) {
				if (update->type != IcebergTableUpdateType::ADD_SNAPSHOT) {
					continue;
				}
				// we need to recreate the keys in the current context.
				auto &ic_table_entry = table.GetLatestSchema(temp_context)->Cast<IcebergTableEntry>();
				ic_table_entry.PrepareIcebergScanFromEntry(temp_context);

				auto &add_snapshot = update->Cast<IcebergAddSnapshot>();
				const auto manifest_list_entries = add_snapshot.GetManifestFiles();
				for (const auto &manifest : manifest_list_entries) {
					for (auto &manifest_entry : manifest.manifest_entries) {
						auto &data_file = manifest_entry.data_file;
						if (fs.TryRemoveFile(data_file.file_path)) {
							DUCKDB_LOG(temp_context, IcebergLogType,
							           "Iceberg Transaction Cleanup, deleted 'data_file': '%s'", data_file.file_path);
						}
					}
				}
			}
		}
	}
}

void IcebergTransaction::Rollback() {
	CleanupFiles();
}

IcebergTransaction &IcebergTransaction::Get(ClientContext &context, Catalog &catalog) {
	D_ASSERT(catalog.GetCatalogType() == "iceberg");
	return Transaction::Get(context, catalog).Cast<IcebergTransaction>();
}

bool IcebergTransaction::StartedBefore(timestamp_t timestamp_ms) const {
	auto ctx = context.lock();
	auto &meta_transaction = MetaTransaction::Get(*ctx);
	auto meta_transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();
	auto start = Timestamp::GetEpochMs(meta_transaction_start);
	return start < timestamp_ms.value;
}

optional_ptr<IcebergTransactionTableState> IcebergTransaction::GetLatestTableState(const string &table_key) {
	auto it = current_table_data.find(table_key);
	if (it == current_table_data.end()) {
		return nullptr;
	}
	return it->second;
}

IcebergTransactionTableState &IcebergTransaction::SetLatestTableState(const string &table_key,
                                                                      IcebergTableStatus status) {
	auto it = current_table_data.find(table_key);
	if (it == current_table_data.end()) {
		it = current_table_data.emplace(table_key, IcebergTransactionTableState(nullptr)).first;
	}
	it->second.SetStatus(status);
	return it->second;
}

IcebergTransactionTableState &IcebergTransaction::SetLatestTableState(IcebergTableInformation &table,
                                                                      IcebergTableStatus status) {
	auto table_key = table.GetTableKey();
	auto &state = SetLatestTableState(table_key, status);
	state.SetTable(table);
	return state;
}

IcebergTransactionAlterUpdate &IcebergTransaction::GetOrCreateAlter() {
	if (transaction_updates.empty() || transaction_updates.back()->type != IcebergTransactionUpdateType::ALTER) {
		auto alter_p = make_uniq<IcebergTransactionAlterUpdate>(*this);
		auto &alter = *alter_p;
		transaction_updates.push_back(std::move(alter_p));
		return alter;
	}
	return transaction_updates.back()->Cast<IcebergTransactionAlterUpdate>();
}

IcebergTableInformation &IcebergTransaction::DeleteTable(IcebergTableInformation &table) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);

	unique_ptr<IcebergTransactionDeleteUpdate> delete_update;
	if (state) {
		auto &table_info = state->GetInfo();
		delete_update = make_uniq<IcebergTransactionDeleteUpdate>(*this, table_info);
	} else {
		delete_update = make_uniq<IcebergTransactionDeleteUpdate>(*this, table);
	}
	auto &deleted_table = delete_update->deleted_table;
	state = SetLatestTableState(deleted_table, IcebergTableStatus::DROPPED);
	transaction_updates.push_back(std::move(delete_update));
	return state->GetInfo();
}

IcebergTableInformation &IcebergTransaction::RenameTable(IcebergTableInformation &table, const string &new_name) {
	auto table_key = table.GetTableKey();
	auto state = GetLatestTableState(table_key);
	if (state) {
		auto &original_table = state->GetInfo();
		if (original_table.HasTransactionUpdates()) {
			throw CatalogException("This table (%s) was modified already, can't be renamed!", table.name);
		}
	}

	state = SetLatestTableState(table, IcebergTableStatus::RENAMED);

	//! Create the rename update, creating the new IcebergTableInformation in the process
	auto rename = make_uniq<IcebergTransactionRenameUpdate>(*this, state->GetInfo(), new_name);
	auto &rename_update = *rename;
	transaction_updates.push_back(std::move(rename));

	//! Update the state of the renamed table
	auto &new_table = rename_update.new_table;
	SetLatestTableState(new_table, IcebergTableStatus::ALIVE);
	new_table.InitSchemaVersions();

	auto locked_context = context.lock();
	auto &client_context = *locked_context;
	//! FIXME: just like the other place, this can easily go wrong
	//! Migrate the MetadataCache
	auto new_table_key = new_table.GetTableKey();
	auto &table_request_cache = catalog.table_request_cache;
	lock_guard<mutex> cache_guard(table_request_cache.Lock());
	auto cache = table_request_cache.Get(client_context, table_key, cache_guard, false);
	table_request_cache.SetOrOverwriteInternal(cache_guard, client_context, new_table_key, cache->expire_timestamp,
	                                           std::move(cache->load_table_result));
	table_request_cache.ExpireInternal(cache_guard, client_context, table_key);
	return state->GetInfo();
}

void ApplyTableUpdate(IcebergTableInformation &table_info, IcebergTransaction &iceberg_transaction,
                      const std::function<void(IcebergTableInformation &)> &callback) {
	auto &alter = iceberg_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_info);
	callback(updated_table);
}

} // namespace duckdb
