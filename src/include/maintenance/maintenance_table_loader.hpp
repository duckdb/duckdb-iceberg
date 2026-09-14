#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

class IcebergCatalog;
struct IcebergTable;

IcebergCatalog &GetMaintenanceIcebergCatalog(ClientContext &context, const QualifiedName &table_name,
                                             const string &function_name);

//! Load metadata into a new table-information instance instead of reusing an
//! already-filled catalog entry. Set force_refresh to bypass the catalog cache.
shared_ptr<IcebergTable> ReloadIcebergTableShared(ClientContext &context, const QualifiedName &table_name,
                                                  const string &function_name, bool force_refresh = false);

} // namespace duckdb
