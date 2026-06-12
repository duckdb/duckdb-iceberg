#include "catalog/rest/transaction/iceberg_transaction.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/main/client_data.hpp"
#include "yyjson.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
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
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "catalog/rest/api/iceberg_commit_exceptions.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"

#include <thread>
#include <chrono>
#include <random>
#include <algorithm>

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
		} else if (requirement.has_assert_table_uuid) {
			auto &assert_table_uuid = requirement.assert_table_uuid;
			auto requirement_json = yyjson_mut_arr_add_obj(doc, requirements_array);
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "type", assert_table_uuid.type.value.c_str());
			yyjson_mut_obj_add_strcpy(doc, requirement_json, "uuid", assert_table_uuid.uuid.c_str());
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
				// we need to recreate the keys in the current context. Use the CURRENT schema (no
				// read-time snapshot-isolation rewind): a commit always targets the latest metadata,
				// and the isolation path would mis-resolve / error (metadata-log) against a concurrent
				// writer's fresher metadata during retry.
				auto &ic_table_entry = table_info.GetCurrentSchemaEntry()->Cast<IcebergTableEntry>();
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

		if (!info.has_assert_create && commit_state.table_info.HasTransactionUpdates()) {
			// ensure table hasn't been swapped by another one with the same name
			auto uuid_requirement = AssertTableUUIDRequirement(table_info);
			uuid_requirement.CreateRequirement(db, context, commit_state);
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

		//! Carry the metadata files written this attempt over to the (persistent) transaction data so
		//! CleanupFiles can delete them if the commit ultimately fails. Accumulates across retries:
		//! each discarded attempt's manifests/manifest-lists are recorded and cleaned on abort.
		for (auto &path : commit_state.written_metadata_paths) {
			transaction_data.written_metadata_paths.push_back(std::move(path));
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
	} catch (IcebergCommitException &ex) {
		//! When the commit outcome is UNKNOWN (e.g. timeout / 5xx) the server may have applied the
		//! commit. Deleting our data files would then corrupt the committed snapshot, so we must NOT
		//! clean up -- surface the error and let an orphan-file maintenance job handle anything that
		//! truly did not land (design doc B11/B13). CONFLICT/FATAL are definite non-commits, so the
		//! normal cleanup applies.
		ErrorData error(ex);
		if (ex.outcome != IcebergCommitOutcome::UNKNOWN) {
			CleanupFiles();
		}
		DropSecrets(*temp_con_context);
		temp_con.Rollback();
		error.Throw("Failed to commit Iceberg transaction: ");
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

	//! Optimistic-concurrency commit (#786). The whole transaction commit is the retry unit: on a
	//! conflict we reload each table's metadata, regenerate manifests/snapshots against the refreshed
	//! parent, and re-submit, with exponential backoff, up to commit.retry.num-retries. When the
	//! transaction commits multiple tables atomically they share this one loop, so fold every table's
	//! commit.retry.* into the most lenient policy rather than honoring an arbitrary "first" table.
	IcebergRetryConfig retry_config {0, 100, 60000, 1800000};
	bool have_config = false;
	for (auto &it : alter_update.updated_tables) {
		auto table_config = IcebergRetryConfig::FromTableMetadata(it.second.table_metadata);
		retry_config = have_config ? retry_config.MostLenient(table_config) : table_config;
		have_config = true;
	}

	//! Cap the cumulative time spent across retries (commit.retry.total-timeout-ms), mirroring Java.
	auto retry_start = std::chrono::steady_clock::now();
	//! Per-commit RNG for backoff jitter. Seeded from a real entropy source + this object's address
	//! so thousands of concurrent writers (each its own process/thread) draw independent jitter and
	//! do not wake in lockstep to re-collide.
	std::mt19937_64 jitter_rng(std::random_device {}() ^
	                           static_cast<uint64_t>(reinterpret_cast<uintptr_t>(this)) ^
	                           static_cast<uint64_t>(
	                               std::chrono::steady_clock::now().time_since_epoch().count()));
	std::uniform_real_distribution<double> jitter_dist(0.0, 1.0);
	//! Decorrelated-jitter state: starts at min_wait so the first retries are tight, then widens.
	int64_t prev_sleep_ms = retry_config.min_wait_ms;

	for (idx_t attempt = 0;; attempt++) {
		try {
			SubmitTableUpdates(alter_update, context);
			break;
		} catch (IcebergCommitException &ex) {
			if (ex.outcome == IcebergCommitOutcome::UNKNOWN) {
				//! Ambiguous outcome (timeout / 5xx): the commit may or may not have landed. Re-load
				//! the table(s) and look for our stable snapshot id(s) to resolve it deterministically.
				switch (CheckCommitStatus(alter_update, context)) {
				case StatusCheckResult::ALL_COMMITTED:
					//! It actually committed; treat as success.
					for (auto &it : alter_update.updated_tables) {
						alter_update.committed_tables.insert(it.first);
					}
					goto committed;
				case StatusCheckResult::NONE_COMMITTED:
					//! Provably not committed: fall through to the CONFLICT-style retry path below.
					break;
				case StatusCheckResult::UNKNOWN:
					//! Cannot prove success or failure -> surface without cleaning up any files.
					throw;
				}
			} else if (ex.outcome != IcebergCommitOutcome::CONFLICT) {
				//! FATAL: definite, non-retryable failure -> abort.
				throw;
			}
			if (attempt >= retry_config.num_retries) {
				throw;
			}
			//! Reload metadata and reset per-table apply state. If no table's parent actually moved,
			//! retrying would hit the same conflict, so stop and surface it.
			if (!RefreshForRetry(alter_update, context)) {
				throw;
			}
			//! Decide how long to wait before the next attempt. Prefer a server-supplied Retry-After
			//! (e.g. a rate-limited 429/503): honoring it avoids hammering a server earlier than it
			//! asked, which is exactly what relieves contention under a herd of concurrent writers.
			//! Cap it at max_wait_ms so a hostile/huge value cannot stall the writer. With no
			//! Retry-After, use decorrelated jitter: tight (near min_wait) for the first retries -- the
			//! refresh+recommit window after a conflict is short, so retrying quickly usually wins --
			//! widening only as repeated failures accumulate, which is when a herd needs spreading.
			int64_t wait_ms;
			if (ex.retry_after_ms >= 0) {
				wait_ms = std::min(ex.retry_after_ms, retry_config.max_wait_ms);
			} else {
				wait_ms = retry_config.DecorrelatedBackoffMs(prev_sleep_ms, jitter_dist(jitter_rng));
				prev_sleep_ms = wait_ms;
			}
			//! Enforce the total retry-time budget against the wait we are about to perform: if it
			//! would exceed commit.retry.total-timeout-ms (measured from the first attempt), stop
			//! instead of sleeping past the budget. Compare without adding (both values are
			//! user/server-influenced and could overflow int64 if summed). Like Java's
			//! Tasks.runTaskWithRetry, the budget only aborts *after* at least one retry (attempt > 0):
			//! a misconfigured tiny total-timeout must not turn the very first conflict into a hard
			//! failure that never retries at all.
			auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
			                      std::chrono::steady_clock::now() - retry_start)
			                      .count();
			if (attempt > 0 && (wait_ms > retry_config.total_wait_ms ||
			                    elapsed_ms > retry_config.total_wait_ms - wait_ms)) {
				throw;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
		}
	}
committed:

	//! NOTE: the per-table metadata cache is refreshed by the commit functions themselves, which
	//! refill it from the commit response's LoadTableResult (the just-committed metadata). That is
	//! strictly better than expiring it here (which would force the next operation to GET) -- the
	//! next read/insert reuses the fresh post-commit metadata, mirroring Java's updateCurrentMetadata.
	DropSecrets(context);
}

//! Reset every table's per-attempt apply state ahead of a retry, and advance its metadata to the
//! latest committed state by re-loading it from the catalog. Returns true if every uncommitted table
//! was successfully refreshed (so a retry can proceed); false only if a table could not be reloaded
//! at all (then retrying is pointless and we surface the conflict).
//!
//! NOTE: we deliberately do NOT require the refreshed snapshot id to have *advanced*. Under heavy
//! concurrency (e.g. thousands of writers appending to one table) a CONFLICT proves someone moved
//! the ref, but a freshly-loaded snapshot can still read as unchanged due to catalog read-replica lag
//! or caching. Refusing to retry in that case (the old behavior) would spuriously abort a writer that
//! should just try again. The server-side assert-ref-snapshot-id on the next attempt is the real
//! correctness guard: if nothing truly moved, the next attempt simply succeeds; if it did, we either
//! succeed against the new parent or conflict again and keep retrying within the configured budget.
//!
//! This is the optimistic-concurrency re-base (#786): we fetch fresh table metadata, re-point the
//! transaction's view of the table at it (keeping our pending data files / transaction_data), and
//! drop cached state so the next apply regenerates manifests/snapshot against the new parent. The
//! commit happens on a dedicated commit-time connection, so re-pointing at the newest metadata here
//! is correct rather than a snapshot-isolation violation.
bool IcebergTransaction::RefreshForRetry(IcebergTransactionAlterUpdate &alter_update, ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	for (auto &it : alter_update.updated_tables) {
		auto &table_info = it.second;
		if (!table_info.transaction_data) {
			continue;
		}
		if (alter_update.committed_tables.count(it.first)) {
			//! Already committed in a previous (single-table fallback) attempt; do not re-apply.
			continue;
		}

		//! Fetch the latest metadata for this table.
		auto &schema = table_info.schema;
		auto load_result = IRCAPI::GetTable(context, ic_catalog, schema, table_info.name);
		if (load_result.has_error) {
			//! Cannot refresh -> cannot safely retry; let the caller surface the conflict.
			return false;
		}

		//! Re-point this table at the freshly loaded metadata. Keep schema versions (they were set
		//! up for this transaction); only the metadata/snapshot baseline moves forward.
		table_info.InitializeFromLoadTableResult(*load_result.result_, false);

		//! Delete the manifest / manifest-list files written by the attempt that just hit a CONFLICT.
		//! A 409 is a definite non-commit, so these are guaranteed orphans -- removing them now avoids
		//! leaking a losing attempt's metadata (the data files are kept; they are reused next attempt).
		//! Skip when the catalog manages its own GC (allows_deletes == false).
		auto &written = table_info.transaction_data->written_metadata_paths;
		if (catalog.attach_options.allows_deletes && !written.empty()) {
			auto &fs = FileSystem::GetFileSystem(context);
			for (auto &path : written) {
				if (fs.TryRemoveFile(path)) {
					DUCKDB_LOG(context, IcebergLogType,
					           "Iceberg retry cleanup, deleted losing-attempt metadata file: '%s'", path);
				}
			}
		}
		written.clear();

		//! Drop cached parent manifest list / row-id baseline so the next apply re-reads them from
		//! the refreshed snapshot, and refresh the catalog cache entry. Pass the live commit-time
		//! context: the re-read resolves storage secrets, which needs an active transaction (the
		//! transaction_data's own context, captured during the operator, is not active here).
		table_info.transaction_data->RefreshForRetry(context);
		ic_catalog.table_request_cache.Expire(context, it.first);

		//! For DELETE/OVERWRITE, verify the data files we target still exist in the refreshed parent;
		//! a concurrent removal makes a retry unsafe and throws (non-retryable). Done after the parent
		//! manifest list was re-read above so it validates against the latest committed state.
		ValidateRetrySafe(table_info, context);
	}
	//! Every uncommitted table refreshed successfully -> a retry can proceed.
	return true;
}

//! Resolve an ambiguous (UNKNOWN) commit outcome by re-loading every uncommitted table and checking
//! whether this transaction's stable snapshot id(s) are now present. Mirrors Java's status-check
//! (`commit.status-check.*`), adapted to REST: we look for our own snapshot ids rather than a
//! metadata-location, because this extension does not own metadata-file writing.
//!
//! ALL_COMMITTED: every uncommitted table has all of its pending snapshot ids present -> the commit
//! actually landed despite the ambiguous transport failure. NONE_COMMITTED: no table has any of its
//! ids -> the server provably did not apply anything, so the attempt can be retried as a conflict.
//! UNKNOWN: a mix (or a load failure) -> we genuinely cannot tell; surface it and clean nothing.
IcebergTransaction::StatusCheckResult
IcebergTransaction::CheckCommitStatus(IcebergTransactionAlterUpdate &alter_update, ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	idx_t expected = 0;  // pending snapshot ids across all uncommitted tables
	idx_t found = 0;     // how many of them are present in the refreshed metadata

	for (auto &it : alter_update.updated_tables) {
		auto &table_info = it.second;
		if (!table_info.transaction_data) {
			continue;
		}
		if (alter_update.committed_tables.count(it.first)) {
			continue;
		}
		auto &alters = table_info.transaction_data->alters;
		if (alters.empty()) {
			continue;
		}

		//! Re-load the latest committed metadata for this table.
		APIResult<unique_ptr<const rest_api_objects::LoadTableResult>> load_result;
		try {
			load_result = IRCAPI::GetTable(context, ic_catalog, table_info.schema, table_info.name);
		} catch (std::exception &) {
			//! A transport-level failure here (GetTable throws rather than setting has_error) means we
			//! still cannot determine whether the commit landed -> stay UNKNOWN so the caller surfaces
			//! the ambiguity and never deletes possibly-committed data files.
			return StatusCheckResult::UNKNOWN;
		}
		if (load_result.has_error) {
			//! Cannot determine the table state -> cannot prove success or failure.
			return StatusCheckResult::UNKNOWN;
		}
		auto refreshed = IcebergTableMetadata::FromTableMetadata(load_result.result_->metadata);

		for (auto &add_snapshot : alters) {
			auto sid = add_snapshot.get().GetSnapshotId();
			expected++;
			if (sid >= 0 && refreshed.snapshots.count(sid)) {
				found++;
			}
		}
	}

	if (expected == 0) {
		//! Nothing was pending (should not happen on this path); treat as not committed.
		return StatusCheckResult::NONE_COMMITTED;
	}
	if (found == expected) {
		return StatusCheckResult::ALL_COMMITTED;
	}
	if (found == 0) {
		return StatusCheckResult::NONE_COMMITTED;
	}
	return StatusCheckResult::UNKNOWN;
}

//! Extract the data-file path a DELETE manifest entry applies to. V3 records it explicitly in
//! `referenced_data_file`; V2 positional deletes do not (per spec) but store the data file path in
//! the FILENAME_FIELD_ID column bounds (lower == upper for a single-file delete), which is exactly
//! how we write them (iceberg_delete.cpp) and how the scan path associates them. Returns "" when the
//! entry cannot be tied to a single data file (e.g. an equality delete, which has no referenced file).
static string DeleteEntryReferencedDataFile(const IcebergManifestEntry &entry) {
	auto &data_file = entry.data_file;
	if (!data_file.referenced_data_file.empty()) {
		return data_file.referenced_data_file;
	}
	auto lower = data_file.lower_bounds.find(MultiFileReader::FILENAME_FIELD_ID);
	auto upper = data_file.upper_bounds.find(MultiFileReader::FILENAME_FIELD_ID);
	if (lower == data_file.lower_bounds.end() || upper == data_file.upper_bounds.end()) {
		return "";
	}
	if (lower->second.IsNull() || upper->second.IsNull()) {
		return "";
	}
	auto lower_str = lower->second.ToString();
	if (lower_str != upper->second.ToString()) {
		//! Bounds span more than one data file -- cannot attribute to a single file conservatively.
		return "";
	}
	return lower_str;
}

//! Before retrying a delete/overwrite, validate it is still safe to replay against the refreshed
//! parent. Three checks, all local (no new catalog API), modeled on Java's validationHistory:
//!  1. Ancestry/rollback guard: the refreshed parent must still descend from the snapshot we started
//!     against (a concurrent rollback to unrelated history makes our computed deletes meaningless).
//!  2. validateDataFilesExist: every data file we add deletes against must still be live in the
//!     refreshed parent (a concurrent commit may have removed it entirely).
//!  3. validateNoNewDeletes: no concurrent commit may have added a *new* delete against any of those
//!     same data files after we started -- our positional/DV deletes were computed against the old
//!     live-row set, so an overlapping concurrent delete could double-delete or invalidate positions.
//! Pure appends (no deletes, no altered_manifests) have no conflict surface and return immediately.
void IcebergTransaction::ValidateRetrySafe(IcebergTableInformation &table_info, ClientContext &context) {
	auto &transaction_data = *table_info.transaction_data;

	//! Collect the data files this transaction adds deletes against. Two sources, unioned:
	//!  - altered_manifests: the V3 deletion-vector replacement path records invalidated data files.
	//!  - the pending DELETE manifest entries (referenced via DeleteEntryReferencedDataFile, which
	//!    handles both V3 referenced_data_file and V2 filename bounds), so we derive the targeted set
	//!    directly from what we are about to commit -- covering V2 positional deletes too.
	unordered_set<string> targeted;
	for (auto &add_snapshot : transaction_data.alters) {
		for (auto &path : add_snapshot.get().altered_manifests.GetInvalidatedFiles()) {
			targeted.insert(path);
		}
		for (auto &manifest_list_entry : add_snapshot.get().GetManifestFiles()) {
			if (manifest_list_entry.file.content != IcebergManifestContentType::DELETE) {
				continue;
			}
			for (auto &entry : manifest_list_entry.manifest_entries) {
				auto referenced = DeleteEntryReferencedDataFile(entry);
				if (!referenced.empty()) {
					targeted.insert(referenced);
				}
			}
		}
	}
	if (targeted.empty()) {
		//! Pure append (or nothing to remove): no conflict surface to validate.
		return;
	}

	auto &metadata = table_info.table_metadata;

	//! Ancestry / rollback guard: the refreshed parent must still descend from the snapshot this
	//! transaction started against. If it does not (e.g. a concurrent rollback moved the ref to an
	//! unrelated history), our delete/overwrite was computed against state no longer on the table's
	//! line, so replaying it is unsafe -> fail non-retryable. Skipped when there was no starting
	//! snapshot (first commit on an empty table has no parent to diverge from). Mirrors Java's
	//! validationHistory ancestry check.
	auto starting_id = transaction_data.starting_snapshot_id;
	int64_t starting_sequence_number = -1;
	if (starting_id >= 0) {
		auto refreshed = metadata.GetLatestSnapshot();
		auto refreshed_id = refreshed ? refreshed->snapshot_id : -1;
		if (refreshed_id < 0 || !metadata.IsAncestorOf(starting_id, refreshed_id)) {
			throw IcebergCommitException(
			    IcebergCommitOutcome::FATAL,
			    StringUtil::Format("Cannot retry commit on table '%s': the table history diverged from the snapshot "
			                       "this statement started against (snapshot %lld is no longer an ancestor of the "
			                       "current snapshot, e.g. a concurrent rollback). Re-run the statement.",
			                       table_info.name, (long long)starting_id));
		}
		//! Remember the sequence number at our starting point: any delete with a higher sequence
		//! number in the refreshed parent was added concurrently (check 3 below). Use a non-throwing
		//! lookup: IsAncestorOf can match on the ancestor id without that id being a key in the map
		//! (an elided snapshot reached as a parent link), and GetSnapshotById would throw. If absent,
		//! leave starting_sequence_number = -1, which simply disables the sequence-based narrowing in
		//! check 3 (still correct, just less precise).
		auto starting_it = metadata.snapshots.find(starting_id);
		if (starting_it != metadata.snapshots.end()) {
			starting_sequence_number = starting_it->second.sequence_number;
		}
	}

	//! Scan the refreshed parent's manifests once: collect live DATA files (for check 2) and any
	//! DELETE entries that reference a targeted data file with a sequence number newer than our
	//! starting point (for check 3). existing_manifest_list was re-read in RefreshForRetry.
	//! All DATA+DELETE manifests are scanned in a single AvroScan (the multi-file reader routes each
	//! manifest's entries back to its own vector slot), rather than one scan per manifest.
	auto &fs = FileSystem::GetFileSystem(context);
	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = metadata.GetLatestSnapshot();
	snapshot_info.schema_id = metadata.GetCurrentSchemaId();

	vector<IcebergManifestListEntry> to_scan;
	for (auto &manifest_list_entry : transaction_data.existing_manifest_list) {
		auto content = manifest_list_entry.file.content;
		if (content == IcebergManifestContentType::DATA || content == IcebergManifestContentType::DELETE) {
			to_scan.push_back(manifest_list_entry);
		}
	}

	unordered_set<string> live_files;
	if (!to_scan.empty()) {
		IcebergOptions options;
		auto scan = AvroScan::ScanManifest(snapshot_info, to_scan, options, fs, "", metadata, context);
		auto reader = make_uniq<manifest_file::ManifestReader>(*scan);
		while (!reader->Finished()) {
			reader->Read();
		}
		for (auto &scanned : to_scan) {
			bool is_data = scanned.file.content == IcebergManifestContentType::DATA;
			for (auto &entry : scanned.manifest_entries) {
				if (entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				if (is_data) {
					live_files.insert(entry.data_file.file_path);
					continue;
				}
				//! DELETE manifest entry: if it targets a file we also delete and was added after we
				//! started, a concurrent commit raced us with an overlapping delete -> unsafe to retry.
				auto referenced = DeleteEntryReferencedDataFile(entry);
				if (referenced.empty() || !targeted.count(referenced)) {
					continue;
				}
				if (starting_sequence_number >= 0) {
					auto entry_sequence = entry.GetSequenceNumber(scanned.file);
					if ((int64_t)entry_sequence <= starting_sequence_number) {
						//! This delete existed at or before we started -- it is part of the state our
						//! deletes already accounted for, not a concurrent addition.
						continue;
					}
				}
				throw IcebergCommitException(
				    IcebergCommitOutcome::FATAL,
				    StringUtil::Format(
				        "Cannot retry commit on table '%s': a concurrent commit added a new delete for "
				        "data file '%s', which this statement also deletes from. Re-run the statement "
				        "against the current table state.",
				        table_info.name, referenced));
			}
		}
	}

	//! Check 2: any targeted file that is no longer live was removed by a concurrent commit.
	for (auto &path : targeted) {
		if (!live_files.count(path)) {
			throw IcebergCommitException(
			    IcebergCommitOutcome::FATAL,
			    StringUtil::Format("Cannot retry commit on table '%s': data file targeted for deletion was "
			                       "concurrently removed: '%s'. Re-run the statement against the current table state.",
			                       table_info.name, path));
		}
	}
}

//! A single commit attempt: build the REST request from the current (possibly refreshed) state and
//! submit it. Re-applies all uncommitted tables; safe to call repeatedly during a retry loop.
void IcebergTransaction::SubmitTableUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context) {
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
		//! The multi-table endpoint's response shape is catalog-dependent and not parsed into
		//! per-table metadata here, so (unlike the single-table path, which refills the cache from its
		//! response) expire the committed tables' cache entries to force a fresh read next time.
		auto &ic_catalog = catalog.Cast<IcebergCatalog>();
		bool expire_cache = ic_catalog.attach_options.max_table_staleness_micros.IsValid();
		for (auto &it : alter_update.updated_tables) {
			alter_update.committed_tables.insert(it.first);
			if (expire_cache) {
				ic_catalog.table_request_cache.Expire(context, it.first);
			}
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

		IRCAPI::CommitNamespacePropertiesUpdate(context, ic_catalog, create_body, namespace_identifiers);
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
			//! Also delete the manifest / manifest-list files this transaction wrote (across all
			//! attempts). These are only reached when the commit did not succeed, so removing them
			//! reclaims the metadata files instead of leaking them. Data files were handled above.
			for (auto &path : transaction_data->written_metadata_paths) {
				if (fs.TryRemoveFile(path)) {
					DUCKDB_LOG(temp_context, IcebergLogType,
					           "Iceberg Transaction Cleanup, deleted metadata file: '%s'", path);
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
