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

//! A view loaded from the catalog. It refuses to run when a table name in its query would be read from outside the
//! view's namespace, or when the columns its * selects no longer match the view's stored columns.
class IcebergViewEntry : public ViewCatalogEntry {
public:
	IcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info, ClientContext &context);
	const SelectStatement &GetQuery() override;
	void UpdateBinding(const vector<LogicalType> &types, const vector<Identifier> &names) override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

private:
	void CheckTableNames(ClientContext &context);

private:
	//! The session reading the view: the binder resolves the view's table names with its search path
	weak_ptr<ClientContext> reader;
	//! Whether the output columns of the view's query come from expanding *
	bool columns_from_star;
};

} // namespace duckdb
