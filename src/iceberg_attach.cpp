#include "iceberg_attach.hpp"
#include "catalog/rest/iceberg_catalog.hpp"

#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/storage/authorization/oauth2.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "catalog/rest/storage/authorization/none.hpp"
#include "regex"

namespace duckdb {

namespace {

static IcebergEndpointType EndpointTypeFromString(const string &input) {
	D_ASSERT(StringUtil::Lower(input) == input);

	static const case_insensitive_map_t<IcebergEndpointType> mapping {{"glue", IcebergEndpointType::AWS_GLUE},
	                                                                  {"s3_tables", IcebergEndpointType::AWS_S3TABLES}};

	for (auto &entry : mapping) {
		if (entry.first == input) {
			return entry.second;
		}
	}
	set<string> options;
	for (auto &entry : mapping) {
		options.insert(entry.first);
	}
	throw InvalidConfigurationException("Unrecognized 'endpoint_type' (%s), accepted options are: %s", input,
	                                    StringUtil::Join(options, ", "));
}

static void S3OrGlueAttachInternal(IcebergAttachOptions &input, const string &service, const string &region) {
	if (input.authorization_type != IcebergAuthorizationType::INVALID) {
		throw InvalidConfigurationException("'endpoint_type' can not be combined with 'authorization_type'");
	}

	input.authorization_type = IcebergAuthorizationType::SIGV4;
	input.endpoint = StringUtil::Format("%s.%s.amazonaws.com/iceberg", service, region);
}

static void S3TablesAttach(IcebergAttachOptions &input) {
	// extract region from the amazon ARN
	auto substrings = StringUtil::Split(input.warehouse, ":");
	if (substrings.size() != 6) {
		throw InvalidInputException("Could not parse S3 Tables ARN warehouse value");
	}
	auto region = substrings[3];
	// Populate sigv4_region so it can be used as a fallback region when creating storage secrets
	input.options.emplace("sigv4_region", Value(region));
	S3OrGlueAttachInternal(input, "s3tables", region);
}

static bool SanityCheckGlueWarehouse(const string &warehouse) {
	// See: https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html#prefix-catalog-path-parameters

	const std::regex patterns[] = {
	    std::regex("^:$"),                  // Default catalog ":" in current account
	    std::regex("^\\d{12}$"),            // Default catalog in a specific account
	    std::regex("^\\d{12}:[^:/]+$"),     // Specific catalog in a specific account
	    std::regex("^[^:]+/[^:]+$"),        // Nested catalog in the current account
	    std::regex("^\\d{12}:[^/]+/[^:]+$") // Nested catalog in a specific account
	};

	for (const auto &pattern : patterns) {
		if (std::regex_match(warehouse, pattern)) {
			return true;
		}
	}

	throw InvalidConfigurationException(
	    "Invalid Glue Catalog Format: '%s'. Expected format: ':', '12-digit account ID', "
	    "'catalog1/catalog2', or '12-digit accountId:catalog1/catalog2'.",
	    warehouse);
}

static void GlueAttach(ClientContext &context, IcebergAttachOptions &input) {
	SanityCheckGlueWarehouse(input.warehouse);

	string secret;
	auto secret_it = input.options.find("secret");
	if (secret_it != input.options.end()) {
		secret = secret_it->second.ToString();
	}

	// look up any s3 secret

	// if there is no secret, an error will be thrown
	auto secret_entry = IcebergCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	auto region = kv_secret.TryGetValue("region");

	if (region.IsNull()) {
		throw InvalidConfigurationException("Assumed catalog secret '%s' for catalog '%s' does not have a region",
		                                    secret_entry->secret->GetName(), input.name);
	}
	S3OrGlueAttachInternal(input, "glue", region.ToString());
}

static void SetAWSCatalogOptions(IcebergAttachOptions &attach_options, case_insensitive_set_t &set_by_attach_options) {
	if (set_by_attach_options.find("remove_files_on_delete") == set_by_attach_options.end()) {
		attach_options.remove_files_on_delete = false;
	}
	if (set_by_attach_options.find("stage_create_tables") == set_by_attach_options.end()) {
		attach_options.stage_create_tables = false;
	}
	if (set_by_attach_options.find("purge_requested") == set_by_attach_options.end()) {
		attach_options.purge_requested = true;
	}
}

} // namespace

unique_ptr<Catalog> IcebergAttach::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &options) {
	IcebergAttachOptions attach_options;
	attach_options.warehouse = info.path;
	attach_options.name = name;

	// check if we have a secret provided
	string default_schema;
	string endpoint_type_string;
	string authorization_type_string;
	string access_mode_string;
	case_insensitive_set_t set_by_attach_options;
	//! First handle generic attach options
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			continue;
		}

		if (lower_name == "endpoint_type") {
			endpoint_type_string = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "authorization_type") {
			authorization_type_string = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "access_delegation_mode") {
			access_mode_string = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "endpoint") {
			attach_options.endpoint = entry.second.ToString();
			StringUtil::RTrim(attach_options.endpoint, "/");
		} else if (lower_name == "stage_create_tables") {
			auto result = entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
			attach_options.stage_create_tables = result;
			set_by_attach_options.insert("stage_create_tables");
		} else if (lower_name == "disable_multi_table_commit") {
			attach_options.disable_multi_table_commit =
			    entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "skip_create_table_metadata_updates") {
			attach_options.skip_create_table_metadata_updates =
			    entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "remove_files_on_delete") {
			attach_options.remove_files_on_delete = entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
			set_by_attach_options.insert("remove_files_on_delete");
		} else if (lower_name == "support_nested_namespaces") {
			attach_options.support_nested_namespaces =
			    entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
			set_by_attach_options.insert("support_nested_namespaces");
		} else if (lower_name == "purge_requested") {
			attach_options.purge_requested = entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
			set_by_attach_options.insert("purge_requested");
		} else if (lower_name == "default_schema") {
			default_schema = entry.second.ToString();
		} else if (lower_name == "encode_entire_prefix") {
			attach_options.encode_entire_prefix = true;
		} else if (lower_name == "max_table_staleness") {
			auto interval_option = entry.second.DefaultCastAs(LogicalType::INTERVAL);
			auto interval_value = interval_option.GetValue<interval_t>();
			int64_t interval_in_micros = 0;
			if (!Interval::TryGetMicro(interval_value, interval_in_micros)) {
				throw ConversionException("Could not get interval information from %s", interval_option.ToString());
			}
			attach_options.max_table_staleness_micros = interval_in_micros;
		} else {
			attach_options.options.emplace(std::move(entry));
		}
	}
	IcebergEndpointType endpoint_type = IcebergEndpointType::INVALID;
	//! Then check any if the 'endpoint_type' is set, for any well known catalogs
	if (!endpoint_type_string.empty()) {
		endpoint_type = EndpointTypeFromString(endpoint_type_string);
		switch (endpoint_type) {
		case IcebergEndpointType::AWS_GLUE: {
			GlueAttach(context, attach_options);
			endpoint_type = IcebergEndpointType::AWS_GLUE;
			SetAWSCatalogOptions(attach_options, set_by_attach_options);
			break;
		}
		case IcebergEndpointType::AWS_S3TABLES: {
			S3TablesAttach(attach_options);
			endpoint_type = IcebergEndpointType::AWS_S3TABLES;
			SetAWSCatalogOptions(attach_options, set_by_attach_options);
			break;
		}
		default:
			throw InternalException("Endpoint type (%s) not implemented", endpoint_type_string);
		}
	}

	//! Then check the authorization type
	if (!authorization_type_string.empty()) {
		if (attach_options.authorization_type != IcebergAuthorizationType::INVALID) {
			throw InvalidConfigurationException("'authorization_type' can not be combined with 'endpoint_type'");
		}
		attach_options.authorization_type = IcebergAuthorization::TypeFromString(authorization_type_string);
	}
	if (!access_mode_string.empty()) {
		if (access_mode_string == "vended_credentials") {
			attach_options.access_mode = IRCAccessDelegationMode::VENDED_CREDENTIALS;
		} else if (access_mode_string == "none") {
			attach_options.access_mode = IRCAccessDelegationMode::NONE;
		} else {
			throw InvalidInputException(
			    "Unrecognized access mode '%s'. Supported options are 'vended_credentials' and 'none'",
			    access_mode_string);
		}
	}
	if (attach_options.authorization_type == IcebergAuthorizationType::INVALID) {
		attach_options.authorization_type = IcebergAuthorizationType::OAUTH2;
	}

	//! Finally, create the auth_handler class from the authorization_type and the remaining options
	unique_ptr<IcebergAuthorization> auth_handler;
	switch (attach_options.authorization_type) {
	case IcebergAuthorizationType::OAUTH2: {
		auth_handler = OAuth2Authorization::FromAttachOptions(db, context, attach_options);
		break;
	}
	case IcebergAuthorizationType::SIGV4: {
		auth_handler = SIGV4Authorization::FromAttachOptions(db, attach_options);
		break;
	}
	case IcebergAuthorizationType::NONE: {
		auth_handler = NoneAuthorization::FromAttachOptions(db, attach_options);
		break;
	}
	default:
		throw InternalException("Authorization Type (%s) not implemented", authorization_type_string);
	}

	//! We throw if there are any additional options not handled by previous steps
	if (!attach_options.options.empty()) {
		set<string> unrecognized_options;
		for (auto &entry : attach_options.options) {
			unrecognized_options.insert(entry.first);
		}
		throw InvalidConfigurationException("Unhandled options found: %s",
		                                    StringUtil::Join(unrecognized_options, ", "));
	}

	if (attach_options.endpoint.empty()) {
		throw InvalidConfigurationException("Missing 'endpoint' option for Iceberg attach");
	}

	D_ASSERT(auth_handler);
	auto catalog =
	    make_uniq<IcebergCatalog>(db, options.access_mode, std::move(auth_handler), attach_options, default_schema);
	//! Remember the raw attach options so that a later ATTACH OR REPLACE can detect when they change.
	catalog->SetAttachOptions(options.options);
	catalog->GetConfig(context, endpoint_type);
	if (!default_schema.empty() && !IRCAPI::VerifySchemaExistence(context, *catalog, default_schema)) {
		throw InvalidConfigurationException("default_schema '%s' does not exist", default_schema);
	}
	return std::move(catalog);
}

} // namespace duckdb
