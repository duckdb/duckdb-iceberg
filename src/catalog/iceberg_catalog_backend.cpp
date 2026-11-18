#include "catalog/iceberg_catalog_backend.hpp"

#include "catalog/rest/api/url_utils.hpp"

namespace duckdb {

IcebergCatalogBackend::IcebergCatalogBackend() {
}

IcebergCatalogBackend::~IcebergCatalogBackend() {
}

IcebergNamespace IcebergCatalogBackend::ParseNamespace(const string &schema_name) const {
	return IcebergNamespace::Parse(schema_name);
}

string IcebergCatalogBackend::GetEntryKey(const IcebergNamespace &ns, const string &name) const {
	if (ns.levels.empty()) {
		return name;
	}
	return IRCPathComponent::NamespaceComponent(ns.levels, "\x1f").encoded + "." + name;
}

shared_ptr<const IcebergLoadedTable> IcebergCatalogBackend::GetCachedTable(ClientContext &context,
                                                                           const string &table_key) {
	return nullptr;
}

void IcebergCatalogBackend::OnTableCreated(const string &table_key, shared_ptr<const IcebergLoadedTable> table) {
}

void IcebergCatalogBackend::OnTableChanged(const string &table_key, optional_ptr<const IcebergLoadedTable> table,
                                           bool dropped) {
}

IcebergCatalogResult<shared_ptr<const IcebergLoadedView>> IcebergCatalogBackend::LoadView(ClientContext &context,
                                                                                          const IcebergEntryId &id) {
	return IcebergCatalogError(
	    IcebergCatalogStatus::UNSUPPORTED,
	    ErrorData(ExceptionType::NOT_IMPLEMENTED, StringUtil::Format("The Iceberg %s does not support views", Name())));
}

const case_insensitive_map_t<string> &IcebergCatalogBackend::GetCatalogProperties() const {
	static const case_insensitive_map_t<string> NO_PROPERTIES;
	return NO_PROPERTIES;
}

} // namespace duckdb
