#pragma once

#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

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

//! A view loaded from the catalog. Refused when it names a table without a namespace, or its * columns changed
class IcebergViewEntry : public ViewCatalogEntry {
public:
	IcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info);
	const SelectStatement &GetQuery() override;
	void UpdateBinding(const vector<LogicalType> &types, const vector<Identifier> &names) override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

private:
	//! Whether the view's columns come from *
	bool columns_from_star;
};

} // namespace duckdb
