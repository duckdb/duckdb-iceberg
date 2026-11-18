#include "iceberg_attach.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

#include "catalog/iceberg_backend_registry.hpp"
#include "catalog/rest/iceberg_catalog.hpp"

namespace duckdb {

IcebergAttachInput::IcebergAttachInput(ClientContext &context, AttachedDatabase &db, const string &name,
                                       AttachInfo &info, AttachOptions &options)
    : context(context), db(db), name(name), info(info), options(options) {
}

bool IcebergAttachInput::ParseGenericOption(const string &lower_name, const Value &argument) {
	if (lower_name == "table_resolution") {
		auto value = StringUtil::Lower(argument.ToString());
		if (value == "lazy") {
			attach_options.table_resolution = IcebergTableResolution::LAZY;
		} else if (value == "eager") {
			attach_options.table_resolution = IcebergTableResolution::EAGER;
		} else {
			throw InvalidConfigurationException(
			    "Unrecognized 'table_resolution' (%s), accepted options are: lazy, eager", value);
		}
	} else if (lower_name == "default_schema") {
		if (argument.IsNull()) {
			default_schema = std::nullopt;
			return true;
		}
		auto schema_name = argument.ToString();
		if (schema_name.empty()) {
			default_schema = std::nullopt;
			return true;
		}
		default_schema = Identifier(schema_name);
	} else if (lower_name == "default_schema_policy") {
		auto value = StringUtil::Lower(argument.ToString());
		if (value == "error") {
			attach_options.default_schema_policy = IcebergDefaultSchemaPolicy::THROW;
		} else if (value == "create") {
			attach_options.default_schema_policy = IcebergDefaultSchemaPolicy::CREATE;
		} else if (value == "unenforced") {
			attach_options.default_schema_policy = IcebergDefaultSchemaPolicy::UNENFORCED;
		} else {
			throw InvalidConfigurationException(
			    "Unrecognized 'default_schema_policy' (%s), accepted options are: error, create, unenforced", value);
		}
	} else if (lower_name == "identifier_case") {
		auto value = StringUtil::Lower(argument.ToString());
		if (value == "preserve_case") {
			attach_options.identifier_case = IcebergIdentifierCase::PRESERVE_CASE;
		} else if (value == "lowercase") {
			attach_options.identifier_case = IcebergIdentifierCase::LOWERCASE;
		} else {
			throw InvalidConfigurationException(
			    "Unrecognized 'identifier_case' (%s), accepted options are: preserve_case, lowercase", value);
		}
	} else {
		return false;
	}
	return true;
}

void IcebergAttachInput::VerifyDefaultSchemaPolicy() const {
	if (!client_options.count("default_schema_policy")) {
		return;
	}
	if (!client_options.count("default_schema")) {
		throw InvalidConfigurationException("DEFAULT_SCHEMA_POLICY requires DEFAULT_SCHEMA");
	}
	// an empty DEFAULT_SCHEMA means no default schema, which leaves nothing to check or create
	if (!default_schema && attach_options.default_schema_policy != IcebergDefaultSchemaPolicy::UNENFORCED) {
		throw InvalidConfigurationException(
		    "DEFAULT_SCHEMA_POLICY 'error' or 'create' requires a non-empty DEFAULT_SCHEMA");
	}
}

static string GetLowercaseOption(const unordered_map<string, Value> &options, const string &name) {
	auto entry = options.find(name);
	if (entry == options.end()) {
		return string();
	}
	return StringUtil::Lower(entry->second.ToString());
}

unique_ptr<Catalog> IcebergAttach::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &options) {
	IcebergAttachInput input(context, db, name, info, options);
	input.client_options = NormalizeIcebergAttachOptions(info.options);
	input.catalog_type = GetLowercaseOption(input.client_options, "catalog_type");
	if (input.catalog_type.empty()) {
		input.catalog_type = "rest";
	}
	input.endpoint_type = GetLowercaseOption(input.client_options, "endpoint_type");

	auto factory = IcebergBackendRegistry::Get(*context.db).Resolve(input);
	auto backend = factory.create(input);
	auto catalog =
	    make_uniq<IcebergCatalog>(db, options.access_mode, input.attach_options, info.path, std::move(backend));
	catalog->SetAttachOptions(options.options);

	auto default_schema = input.default_schema;
	if (default_schema) {
		default_schema = Identifier(catalog->FoldIdentifier(default_schema->GetIdentifierName()));
	}
	catalog->default_schema = default_schema;
	auto policy = input.attach_options.default_schema_policy;
	if (!default_schema) {
		return std::move(catalog);
	}
	auto &schema_name = default_schema->GetIdentifierName();
	auto &catalog_backend = catalog->GetBackend();
	auto ns = catalog_backend.ParseNamespace(schema_name);
	switch (policy) {
	case IcebergDefaultSchemaPolicy::UNENFORCED:
		// no round trip here: IcebergSchemaSet verifies the default schema on its first lookup
		return std::move(catalog);
	case IcebergDefaultSchemaPolicy::THROW:
		if (!catalog_backend.NamespaceExists(context, ns)) {
			throw InvalidConfigurationException(
			    "default_schema '%s' does not exist. ATTACH with no DEFAULT_SCHEMA to successfully attach, or with "
			    "DEFAULT_SCHEMA_POLICY 'create' to create it",
			    schema_name);
		}
		return std::move(catalog);
	case IcebergDefaultSchemaPolicy::CREATE: {
		// check before creating: a create of an existing namespace costs a 409 plus a re-check
		if (catalog_backend.NamespaceExists(context, ns)) {
			return std::move(catalog);
		}
		if (options.access_mode == AccessMode::READ_ONLY) {
			throw InvalidConfigurationException("default_schema '%s' does not exist, and DEFAULT_SCHEMA_POLICY "
			                                    "'create' cannot create it on a READ_ONLY attach",
			                                    schema_name);
		}
		catalog_backend.CreateNamespace(context, ns);
		return std::move(catalog);
	}
	}
	throw InternalException("Unknown IcebergDefaultSchemaPolicy");
}

} // namespace duckdb
