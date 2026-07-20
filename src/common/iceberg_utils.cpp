#include "common/iceberg_utils.hpp"

#include "duckdb.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/common/operator/add.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"

namespace duckdb {

int64_t IcebergUtils::AddFileSizeChecked(int64_t total, int64_t file_size_in_bytes) {
	if (file_size_in_bytes < 0) {
		throw InvalidConfigurationException("Iceberg content file has negative 'file_size_in_bytes': %lld",
		                                    file_size_in_bytes);
	}
	int64_t updated;
	if (!TryAddOperator::Operation(total, file_size_in_bytes, updated)) {
		throw InvalidConfigurationException("Iceberg content file sizes exceed the supported BIGINT range");
	}
	return updated;
}

timestamp_ms_t IcebergUtils::GetTransactionStartTimeMS(ClientContext &context) {
	auto &meta_transaction = MetaTransaction::Get(context);
	auto transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();
	return timestamp_ms_t(Timestamp::GetEpochMs(transaction_start));
}

idx_t IcebergUtils::ParseByteSizeOptionallyFormatted(const string &input) {
	idx_t result;
	auto error = StringUtil::TryParseFormattedBytes(input, result);
	if (error.empty()) {
		return result;
	}

	try {
		auto parsed = std::stoll(input);
		return parsed;
	} catch (...) {
		throw InvalidConfigurationException("Invalid format for 'write.target-file-size-bytes' (%s), error: %s", input,
		                                    error);
	}
}

CopyFunctionCatalogEntry &IcebergUtils::GetCopyFunction(ClientContext &context, const string &name) {
	// Logic is partially duplicated from Catalog::AutoLoadExtensionByCatalogEntry(db, CatalogType::COPY_FUNCTION_ENTRY,
	// name), but that do not offer enough control
	auto &db = *context.db;
	string extension_name = ExtensionHelper::FindExtensionInEntries(name, EXTENSION_COPY_FUNCTIONS);
	if (!extension_name.empty() && Settings::Get<AutoloadKnownExtensionsSetting>(context) &&
	    ExtensionHelper::CanAutoloadExtension(extension_name)) {
		// This will either succeed or throw
		ExtensionHelper::AutoLoadExtension(db, extension_name);
	}
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);

	auto entry = system_catalog.GetEntry<CopyFunctionCatalogEntry>(context, Identifier::DefaultSchema(),
	                                                               Identifier(name), OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		throw MissingExtensionException(
		    "Could not load the copy function for \"%s\". Try explicitly loading the \"%s\" extension", name, name);
	}
	return *entry;
}

idx_t IcebergUtils::CountOccurrences(const string &input, const string &to_find) {
	size_t pos = input.find(to_find);
	idx_t count = 0;
	while (pos != string::npos) {
		pos = input.find(to_find, pos + to_find.length()); // move past current match
		count++;
	}
	return count;
}

string IcebergUtils::FileToString(const string &path, FileSystem &fs) {
	// This function is used to read table metadata files (e.g., metadata.json).
	//
	// We intentionally avoid calling GetFileSize() to pre-allocate the result buffer because
	// GetFileSize() triggers a HEAD request on object stores (S3, GCS, etc.), adding an extra
	// round trip on the critical path of queries.
	//
	// Table metadata is typically small (a few KB) and there is only one per query, so
	// progressively reallocating the result string has minimal cost compared to the latency
	// of an additional network request.
	//
	// We read directly into the result string's buffer to avoid an intermediate copy,
	// growing the buffer 2x when space runs out (amortized O(1) per byte).
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	string result;
	idx_t capacity = 32768;
	idx_t size = 0;
	result.resize(capacity);

	while (true) {
		if (size == capacity) {
			capacity *= 2;
			result.resize(capacity);
		}
		auto bytes_read = handle->Read(&result[size], capacity - size);
		if (bytes_read == 0) {
			break;
		}
		size += bytes_read;
	}
	result.resize(size);
	return result;
}

static string ExtractIcebergScanPath(const string &sql) {
	auto lower_sql = StringUtil::Lower(sql);
	auto start = lower_sql.find("iceberg_scan('");
	if (start == string::npos) {
		throw InvalidInputException("Could not find ICEBERG_SCAN in referenced view");
	}
	start += 14;
	auto end = sql.find("\'", start);
	if (end == string::npos) {
		throw InvalidInputException("Could not find end of the ICEBERG_SCAN in referenced view");
	}
	return sql.substr(start, end - start);
}

optional_ptr<CatalogEntry> IcebergUtils::GetTableEntry(ClientContext &context, string &input_string) {
	auto qualified_name = QualifiedName::ParseComponents(input_string);
	auto default_db = DatabaseManager::GetDefaultDatabase(context);
	auto &catalog = Catalog::GetCatalog(context, Identifier(default_db));
	switch (qualified_name.size()) {
	case 3: {
		auto lookup_info = EntryLookupInfo(CatalogType::TABLE_ENTRY, Identifier(qualified_name[2]));
		auto table = Catalog::GetEntry(context, Identifier(qualified_name[0]), Identifier(qualified_name[1]),
		                               lookup_info, OnEntryNotFound::THROW_EXCEPTION);
		return table;
	}
	case 2: {
		// make sure default catalog is iceberg
		auto table_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, Identifier(qualified_name[0]),
		                                    Identifier(qualified_name[1]), OnEntryNotFound::THROW_EXCEPTION);
		return table_entry;
	}
	case 1: {
		auto schema = catalog.GetDefaultSchema();
		auto table_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, Identifier(schema),
		                                    Identifier(qualified_name[0]), OnEntryNotFound::THROW_EXCEPTION);
		return table_entry;
	}
	default:
		throw InvalidInputException("Too many identifiers in table name %s", input_string);
	}
}

optional_ptr<SchemaCatalogEntry> IcebergUtils::GetSchemaEntry(ClientContext &context, string &input_string) {
	auto qualified_name = QualifiedName::ParseComponents(input_string);
	auto default_db = DatabaseManager::GetDefaultDatabase(context);

	switch (qualified_name.size()) {
	case 1: {
		// input: schema
		// we assume the catalog from context
		auto &catalog = Catalog::GetCatalog(context, Identifier(default_db));
		return catalog.GetSchema(context, Identifier(qualified_name[0]), OnEntryNotFound::THROW_EXCEPTION);
	}
	default: {
		// input: catalog.schema (schema may contain dots for nested namespaces)
		auto catalog_name = qualified_name[0];
		vector<string> schema_parts(qualified_name.begin() + 1, qualified_name.end());
		auto schema_name = StringUtil::Join(schema_parts, ".");
		auto lookup_info = EntryLookupInfo(CatalogType::SCHEMA_ENTRY, Identifier(schema_name));
		auto retriever = CatalogEntryRetriever(context);
		return retriever.GetSchema(Identifier(catalog_name), lookup_info, OnEntryNotFound::THROW_EXCEPTION);
	}
	}
}

string IcebergUtils::GetStorageLocation(ClientContext &context, const string &input) {
	auto qualified_name = QualifiedName::ParseComponents(input);
	string storage_location = input;

	do {
		if (qualified_name.size() != 3) {
			break;
		}
		//! Fully qualified table reference, let's do a lookup
		EntryLookupInfo table_info(CatalogType::TABLE_ENTRY, Identifier(qualified_name[2]));
		auto catalog_entry = Catalog::GetEntry(context, Identifier(qualified_name[0]), Identifier(qualified_name[1]),
		                                       table_info, OnEntryNotFound::RETURN_NULL);
		if (!catalog_entry) {
			break;
		}
		if (catalog_entry->type == CatalogType::VIEW_ENTRY) {
			//! This is a view, which we will assume is wrapping an ICEBERG_SCAN(...) query
			auto &view_entry = catalog_entry->Cast<ViewCatalogEntry>();
			auto &sql = view_entry.sql;
			storage_location = ExtractIcebergScanPath(sql);
			break;
		}
		if (catalog_entry->type == CatalogType::TABLE_ENTRY) {
			//! This is a IRCTableEntry, set up the scan from this
			auto &table = catalog_entry->Cast<TableCatalogEntry>();
			if (table.catalog.GetCatalogType() != "iceberg") {
				throw InvalidInputException("Table %s is not an Iceberg table", input);
			}
			auto &table_entry = catalog_entry->Cast<IcebergTableEntry>();
			storage_location = table_entry.table_info.table_metadata.GetLocation();
			// Prepare Iceberg Scan from entry will create the secret needed to access the table
			table_entry.PrepareIcebergScanFromEntry(context);
			break;
		}
	} while (false);
	return storage_location;
}

IcebergResolvedMetadata IcebergUtils::ResolveTableMetadata(ClientContext &context, const string &input,
                                                           const IcebergOptions &options) {
	auto qualified_name = QualifiedName::ParseComponents(input);
	if (qualified_name.size() == 3) {
		auto table_name =
		    QualifiedName(Identifier(qualified_name[0]), Identifier(qualified_name[1]), Identifier(qualified_name[2]));
		EntryLookupInfo table_info(CatalogType::TABLE_ENTRY, std::move(table_name));
		auto catalog_entry = Catalog::GetEntry(context, table_info, OnEntryNotFound::RETURN_NULL);
		if (catalog_entry && catalog_entry->type == CatalogType::TABLE_ENTRY) {
			auto &table = catalog_entry->Cast<TableCatalogEntry>();
			if (table.catalog.GetCatalogType() != "iceberg") {
				throw InvalidInputException("Table %s is not an Iceberg table", input);
			}

			auto &iceberg_table = catalog_entry->Cast<IcebergTableEntry>();
			iceberg_table.PrepareIcebergScanFromEntry(context);
			auto &metadata = iceberg_table.table_info.table_metadata;
			return IcebergResolvedMetadata(metadata.GetLocation(), metadata.Copy());
		}
	}

	auto table_location = GetStorageLocation(context, input);
	auto &fs = FileSystem::GetFileSystem(context);
	auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(fs, *context.db);
	auto metadata_path = IcebergTableMetadata::GetMetaDataPath(context, table_location, fs, options);
	auto table_metadata = IcebergTableMetadata::Parse(metadata_path, *caching_fs, options.metadata_compression_codec);
	return IcebergResolvedMetadata(std::move(table_location), IcebergTableMetadata::FromTableMetadata(table_metadata));
}

// Function to decompress a gz file content string
string IcebergUtils::GzFileToString(const string &path, FileSystem &fs) {
	// Initialize zlib variables
	string gzipped_string = FileToString(path, fs);
	return GZipFileSystem::UncompressGZIPString(gzipped_string);
}

string IcebergUtils::GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs) {
	auto lpath = StringUtil::Lower(relative_file_path);
	auto found = lpath.rfind("/metadata/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	if (StringUtil::StartsWith(lpath, "data")) {
		found = 0;
	} else {
		found = lpath.rfind("/data/") + 1;
	}

	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found));
	}

	throw InvalidConfigurationException("Could not create full path from Iceberg Path (%s) and the relative path (%s)",
	                                    iceberg_path, relative_file_path);
}

} // namespace duckdb
