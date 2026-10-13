
#include "catalog/rest/storage/iceberg_authorization.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "catalog/rest/api/api_utils.hpp"
#include "catalog/rest/storage/authorization/oauth2.hpp"

namespace duckdb {

IcebergAuthorizationType IcebergAuthorization::TypeFromString(const string &type) {
	static const case_insensitive_map_t<IcebergAuthorizationType> mapping {{"oauth2", IcebergAuthorizationType::OAUTH2},
	                                                                       {"sigv4", IcebergAuthorizationType::SIGV4},
	                                                                       {"none", IcebergAuthorizationType::NONE},
	                                                                       {"azure", IcebergAuthorizationType::AZURE}};

	for (auto it : mapping) {
		if (StringUtil::CIEquals(it.first, type)) {
			return it.second;
		}
	}

	set<string> accepted_options;
	for (auto it : mapping) {
		accepted_options.insert(it.first);
	}
	throw InvalidConfigurationException("'authorization_type' '%s' is not supported, valid options are: %s", type,
	                                    StringUtil::Join(accepted_options, ", "));
}

void IcebergAuthorization::ParseExtraHttpHeaders(const Value &headers_value,
                                                 unordered_map<string, string> &out_headers) {
	if (headers_value.IsNull() || headers_value.type().id() != LogicalTypeId::MAP) {
		return;
	}

	// MAP is internally a LIST<STRUCT(key, value)>
	// Each entry in the list is a STRUCT with exactly two fields: key and value
	auto &map_entries = MapValue::GetChildren(headers_value);

	for (const auto &entry : map_entries) {
		if (entry.type().id() != LogicalTypeId::STRUCT) {
			continue;
		}

		auto &struct_children = StructValue::GetChildren(entry);
		if (struct_children.size() != 2) {
			continue;
		}

		// struct_children[0] = key, struct_children[1] = value
		out_headers[struct_children[0].ToString()] = struct_children[1].ToString();
	}
}

bool IcebergAuthorization::ForceTokenExpiry(ClientContext &context) {
	Value force_expiry_val;
	if (context.TryGetCurrentSetting("iceberg_test_force_token_expiry", force_expiry_val)) {
		return !force_expiry_val.IsNull() && force_expiry_val.type().id() == LogicalTypeId::BOOLEAN &&
		       force_expiry_val.GetValue<bool>();
	}
	return false;
}

bool IcebergAuthorization::ReplaySecretRefresh(ClientContext &context, const SecretEntry &secret_entry) {
	const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry.secret);
	Value refresh_info;
	if (!kv_secret.TryGetValue("refresh_info", refresh_info)) {
		return false;
	}

	// refresh_info holds the named parameters the secret was created with. Replaying them re-runs the
	// credential chain provider, which fetches fresh credentials.
	CreateSecretInput refresh_input;
	refresh_input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	refresh_input.persist_type = SecretPersistType::TEMPORARY;
	refresh_input.type = kv_secret.GetType();
	refresh_input.name = kv_secret.GetName();
	refresh_input.provider = kv_secret.GetProvider();
	refresh_input.storage_type = Identifier(secret_entry.storage_mode);
	refresh_input.scope = kv_secret.GetScope();

	auto child_count = StructType::GetChildCount(refresh_info.type());
	auto children = StructValue::GetChildren(refresh_info);
	for (idx_t i = 0; i < child_count; i++) {
		auto &key = StructType::GetChildName(refresh_info.type(), i);
		refresh_input.options[key.GetIdentifierName()] = children[i];
	}

	auto &secret_manager = context.db->GetSecretManager();
	(void)secret_manager.CreateSecret(context, refresh_input);
	return true;
}

} // namespace duckdb
