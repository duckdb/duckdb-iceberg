#include "catalog/rest/iceberg_catalog.hpp"

#include "duckdb/storage/database_size.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "common/iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/api/api_utils.hpp"
#include "rest_catalog/objects/catalog_config.hpp"

namespace duckdb {

IcebergCatalog::IcebergCatalog(AttachedDatabase &db_p, AccessMode access_mode, IcebergAttachOptions &attach_options_p,
                               string attach_path_p, unique_ptr<IcebergCatalogBackend> backend_p)
    : Catalog(db_p), access_mode(access_mode), attach_options(attach_options_p), attach_path(std::move(attach_path_p)),
      backend(std::move(backend_p)), schemas(*this) {
	D_ASSERT(backend);
}

IcebergCatalog::~IcebergCatalog() = default;

//===--------------------------------------------------------------------===//
// Catalog API
//===--------------------------------------------------------------------===//

void IcebergCatalog::Initialize(bool load_builtin) {
}

void IcebergCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<IcebergSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> IcebergCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
	auto schema_name = FoldIdentifier(schema_lookup.GetEntryName());
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name, if_not_found);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw CatalogException(schema_lookup.GetErrorContext(), "Schema with name \"%s\" not found", schema_name);
	}

	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

optional<Identifier> IcebergCatalog::GetDefaultSchema() const {
	return default_schema;
}

string IcebergCatalog::FoldIdentifier(const string &name) const {
	if (attach_options.identifier_case == IcebergIdentifierCase::LOWERCASE) {
		return StringUtil::Lower(name);
	}
	return name;
}

QualifiedName IcebergCatalog::FoldName(const QualifiedName &name) const {
	if (attach_options.identifier_case == IcebergIdentifierCase::PRESERVE_CASE) {
		return name;
	}
	auto path = name.Path();
	for (idx_t i = name.Catalog().empty() ? 0 : 1; i < path.size(); i++) {
		path[i] = Identifier(FoldIdentifier(path[i].GetIdentifierName()));
	}
	return QualifiedName::FromPath(std::move(path));
}

optional_ptr<CatalogEntry> IcebergCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	optional_ptr<ClientContext> context = transaction.GetContext();
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException(
		    "CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements");
	}

	D_ASSERT(context);
	info.SetQualifiedName(FoldName(info.GetQualifiedName()));
	auto &iceberg_transaction = IcebergTransaction::Get(*context, *this);
	auto &schema_name = info.GetQualifiedName().Schema().GetIdentifierName();
	auto created_schema = iceberg_transaction.created_schemas.CheckPending(schema_name, info.on_conflict);
	if (created_schema) {
		return created_schema.get();
	}

	// Verify schema existence on the server first
	bool schema_exists = GetBackend().NamespaceExists(*context, GetBackend().ParseNamespace(schema_name));

	if (schema_exists) {
		if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			// Schema already exists on the server - get or create a local entry and return it
			auto entry = schemas.GetEntry(*context, schema_name, OnEntryNotFound::RETURN_NULL);
			if (entry) {
				return entry;
			}
			auto new_schema = make_shared_ptr<IcebergSchemaEntry>(*this, info);
			schemas.AddEntry(schema_name, new_schema);
			iceberg_transaction.schemas[schema_name] = new_schema;
			return new_schema.get();
		}
		throw CatalogException("Schema with name %s already exists", info.GetQualifiedName().Schema());
	}

	// Schema does not exist - stage it locally and defer the server creation and catalog publication to commit
	auto new_schema = make_shared_ptr<IcebergSchemaEntry>(*this, info);
	auto result = new_schema.get();
	iceberg_transaction.created_schemas.Add(schema_name, std::move(new_schema), info.on_conflict);
	return result;
}

void IcebergCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto cascade_only = GetBackend().Capabilities().drop_namespace == IcebergDropNamespaceMode::CASCADE_ONLY;
	if (info.cascade && !cascade_only) {
		throw NotImplementedException(
		    "DROP SCHEMA <schema_name> CASCADE is not supported for Iceberg schemas currently");
	}
	if (!info.cascade && cascade_only) {
		throw NotImplementedException("DROP SCHEMA without CASCADE is not supported for AWS Glue: deleting a namespace "
		                              "also deletes its tables. Use DROP SCHEMA <schema_name> CASCADE");
	}
	info.SetQualifiedName(FoldName(info.GetQualifiedName()));

	// Verify schema existence on the server first
	bool schema_exists = GetBackend().NamespaceExists(
	    context, GetBackend().ParseNamespace(info.GetQualifiedName().Name().GetIdentifierName()));

	if (!schema_exists) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			// remove the entry if it exists locally
			// it could have been created during the bind phase.
			GetSchemas().RemoveEntry(info.GetQualifiedName().Name().GetIdentifierName());
			return;
		}
		throw CatalogException("Schema with name %s does not exist", info.GetQualifiedName().Name());
	}

	// Schema exists - defer the server deletion to commit
	auto &iceberg_transaction = IcebergTransaction::Get(context, *this);
	iceberg_transaction.deleted_schemas.insert(info.GetQualifiedName().Name().GetIdentifierName());
}

unique_ptr<LogicalOperator> IcebergCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                            TableCatalogEntry &table,
                                                            unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("IcebergCatalog BindCreateIndex");
}

bool IcebergCatalog::InMemory() {
	return false;
}

string IcebergCatalog::GetDBPath() {
	return attach_path;
}

DatabaseSize IcebergCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

string IcebergCatalog::GetEntryKey(const vector<string> &namespace_items, const string &name) const {
	return GetBackend().GetEntryKey(IcebergNamespace(namespace_items), name);
}

ErrorData IcebergCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	return ErrorData();
}

unique_ptr<SecretEntry> IcebergCatalog::GetStorageSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	vector<string> accepted_secret_types {"s3", "aws"};

	if (!secret_name.empty()) {
		auto secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
		if (secret_entry) {
			auto secret_type = secret_entry->secret->GetType();
			auto matching_secret = std::find(accepted_secret_types.begin(), accepted_secret_types.end(), secret_type);
			if (matching_secret != accepted_secret_types.end()) {
				return secret_entry;
			}
			throw InvalidConfigurationException(
			    "Found a secret by the name of '%s', but it is not of an accepted type for a 'secret', "
			    "accepted types are: 's3' or 'aws', found %s",
			    secret_name, secret_type);
		}
		throw InvalidConfigurationException(
		    "No secret by the name of '%s' could be found, consider changing the 'secret'", secret_name);
	}

	// Check explicit defaults of both types before considering automatic fallback credentials.
	for (const auto &type : accepted_secret_types) {
		auto secret_entry =
		    context.db->GetSecretManager().GetSecretByName(transaction, StringUtil::Format("__default_%s", type));
		if (secret_entry) {
			return secret_entry;
		}
	}

	SecretMatch best_match;
	for (const auto &type : accepted_secret_types) {
		auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, type + "://", type);
		if (!secret_match.HasMatch()) {
			//! Didn't result in a match at all
			continue;
		}
		if (secret_match.score < best_match.score) {
			//! Lower score, skip
			continue;
		}
		if (secret_match.score != best_match.score) {
			//! Higher score, replace the current best
			best_match = secret_match;
			continue;
		}
		if (secret_match.GetSecret().GetName() < best_match.GetSecret().GetName()) {
			best_match = secret_match;
		}
	}
	if (best_match.HasMatch()) {
		return std::move(best_match.secret_entry);
	}
	throw InvalidConfigurationException("Could not find a valid storage secret (s3 or aws)");
}

IcebergSchemaSet &IcebergCatalog::GetSchemas() {
	return schemas;
}

unique_ptr<SecretEntry> IcebergCatalog::GetIcebergSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	unique_ptr<SecretEntry> secret_entry = nullptr;
	if (secret_name.empty()) {
		//! Try to find any secret with type 'iceberg'
		auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, "", "iceberg");
		if (!secret_match.HasMatch()) {
			return nullptr;
		}
		secret_entry = std::move(secret_match.secret_entry);
	} else {
		secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
	}
	return secret_entry;
}

unique_ptr<SecretEntry> IcebergCatalog::GetHTTPSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	unique_ptr<SecretEntry> secret_entry = nullptr;

	if (!secret_name.empty()) {
		secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
		if (!secret_entry) {
			throw InternalException("Secret '%s' not found", secret_name);
		}
		auto http_kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		bool has_proxy = !http_kv_secret.TryGetValue("http_proxy").IsNull();
		if (has_proxy) {
			return secret_entry;
		}
	}
	auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, "", "http");
	if (!secret_match.HasMatch()) {
		return nullptr;
	}
	secret_entry = std::move(secret_match.secret_entry);
	return secret_entry;
}
//===--------------------------------------------------------------------===//
// Attach
//===--------------------------------------------------------------------===//

//! Streamlined initialization for recognized catalog types

void IcebergCatalog::SetAttachOptions(const unordered_map<string, Value> &options) {
	normalized_attach_options = NormalizeIcebergAttachOptions(options);
}

bool IcebergCatalog::HasConflictingAttachOptions(const string &path, const AttachOptions &options) {
	//! If the base catalog already considers the path or catalog type to conflict, re-attach.
	if (Catalog::HasConflictingAttachOptions(path, options)) {
		return true;
	}
	//! Otherwise compare the iceberg-specific attach options (URI, credentials, MAX_TABLE_STALENESS, ...)
	//! so that ATTACH OR REPLACE re-runs Attach when any of them changes.
	auto normalized_options = NormalizeIcebergAttachOptions(options.options);
	if (normalized_options.size() != normalized_attach_options.size()) {
		return true;
	}
	for (auto &entry : normalized_options) {
		auto it = normalized_attach_options.find(entry.first);
		if (it == normalized_attach_options.end()) {
			return true;
		}
		if (it->second.type() != entry.second.type() || it->second.ToString() != entry.second.ToString()) {
			return true;
		}
	}
	return false;
}

void IcebergCatalog::VerifyMergeOnRead(const IcebergTableMetadata &metadata, const string &table_name,
                                       const string &write_mode_property) {
	if (metadata.AllowsMergeOnRead(write_mode_property)) {
		return;
	}
	throw NotImplementedException(
	    "Table property '%s' is set to '%s' for table %s, and only merge-on-read is supported for deletes, updates "
	    "and merges. Set it with ALTER TABLE %s SET ('%s' = 'merge-on-read')",
	    write_mode_property, metadata.GetTableProperty(write_mode_property), table_name, table_name,
	    write_mode_property);
}

} // namespace duckdb
