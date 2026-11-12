//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/iceberg_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

struct IcebergResolvedMetadata {
	IcebergResolvedMetadata(string table_location_p, IcebergTableMetadata metadata_p)
	    : table_location(std::move(table_location_p)), metadata(std::move(metadata_p)) {
	}

	string table_location;
	IcebergTableMetadata metadata;
};

class IcebergUtils {
public:
	//! Downloads a file fully into a string
	static string FileToString(const OpenFileInfo &file, FileSystem &fs);
	//! Downloads a gz file fully into a string
	static string GzFileToString(const OpenFileInfo &file, FileSystem &fs);
	//! Somewhat hacky function that allows relative paths in iceberg tables to be resolved,
	//! used for the allow_moved_paths debug option which allows us to test with iceberg tables that
	//! were moved without their paths updated
	static string GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs);
	static string GetStorageLocation(ClientContext &context, const string &input);
	static IcebergResolvedMetadata ResolveTableMetadata(ClientContext &context, const string &input,
	                                                    const IcebergOptions &options);
	//! Parse a 'table', 'schema.table' or 'catalog.schema.table' string
	static QualifiedName ParseTableName(const string &input_string);
	//! Look up a table named by a 'table', 'schema.table' or 'catalog.schema.table' string
	static optional_ptr<CatalogEntry> GetTableEntry(ClientContext &context, string &input_string);
	//! Look up a catalog entry of 'type' named by a 'name', 'schema.name' or 'catalog.schema.name' string
	static optional_ptr<CatalogEntry> GetCatalogEntry(ClientContext &context, CatalogType type,
	                                                  const string &input_string, OnEntryNotFound if_not_found);
	//! For functions that take either a table name or a table location: the catalog table the string names, or
	//! nullptr when it names a location (or nothing in the catalog)
	static optional_ptr<CatalogEntry> TryGetCatalogTable(ClientContext &context, const string &input);
	//! The 'catalog.schema.table' name of a table entry
	static QualifiedName GetQualifiedTableName(CatalogEntry &table_entry);
	static optional_ptr<SchemaCatalogEntry> GetSchemaEntry(ClientContext &context, string &input_string);
	static idx_t CountOccurrences(const string &input, const string &to_find);
	static CopyFunctionCatalogEntry &GetCopyFunction(ClientContext &context, const Identifier &name);
	static idx_t ParseByteSizeOptionallyFormatted(const string &input);
	static int64_t AddFileSizeChecked(int64_t total, int64_t file_size_in_bytes);
	static timestamp_ms_t GetTransactionStartTimeMS(ClientContext &context);
	static bool IsIcebergCatalog(Catalog &catalog);
};

} // namespace duckdb
