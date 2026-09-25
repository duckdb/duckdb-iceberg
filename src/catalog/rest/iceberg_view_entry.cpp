#include "catalog/rest/iceberg_view_entry.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

UnsupportedIcebergViewEntry::UnsupportedIcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                         CreateViewInfo &info, string reason)
    : ViewCatalogEntry(catalog, schema, info), reason(std::move(reason)) {
}

const SelectStatement &UnsupportedIcebergViewEntry::GetQuery() {
	throw BinderException("Cannot query Iceberg view '%s': %s", name, reason);
}

void UnsupportedIcebergViewEntry::BindView(ClientContext &context, BindViewAction action) {
	GetQuery();
}

unique_ptr<CreateInfo> UnsupportedIcebergViewEntry::GetInfo() const {
	auto info = make_uniq<CreateViewInfo>(schema, name);
	info->sql = sql;
	info->aliases = aliases;
	return std::move(info);
}

string UnsupportedIcebergViewEntry::ToSQL() const {
	return sql;
}

unique_ptr<CatalogEntry> UnsupportedIcebergViewEntry::Copy(ClientContext &context) const {
	auto info = GetInfo();
	return make_uniq<UnsupportedIcebergViewEntry>(catalog, schema, info->Cast<CreateViewInfo>(), reason);
}

} // namespace duckdb
