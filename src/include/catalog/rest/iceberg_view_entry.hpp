#pragma once

#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "rest_catalog/objects/view_version.hpp"

namespace duckdb {

//! An unsupported view remains visible to catalog inspection and DROP without executable placeholder SQL.
class UnsupportedIcebergViewEntry : public ViewCatalogEntry {
public:
	UnsupportedIcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info, string reason);
	const SelectStatement &GetQuery() override;
	void BindView(ClientContext &context, BindViewAction action) override;
	unique_ptr<CreateInfo> GetInfo() const override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	string ToSQL() const override;

private:
	string reason;
};

//! Resolve omitted table qualifications using the persisted view version, without changing CTE references.
void QualifyIcebergView(SelectStatement &query, const rest_api_objects::ViewVersion &version,
                        const Identifier &owning_catalog);

} // namespace duckdb
