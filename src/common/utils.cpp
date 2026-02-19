#include "duckdb.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "iceberg_utils.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"
#include "storage/iceberg_table_information.hpp"
#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {

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
	auto &catalog = Catalog::GetCatalog(context, default_db);
	switch (qualified_name.size()) {
	case 3: {
		auto lookup_info = EntryLookupInfo(CatalogType::TABLE_ENTRY, qualified_name[2]);
		auto table = Catalog::GetEntry(context, qualified_name[0], qualified_name[1], lookup_info,
		                               OnEntryNotFound::THROW_EXCEPTION);
		return table;
	}
	case 2: {
		// make sure default catalog is iceberg
		auto table_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, qualified_name[0], qualified_name[1],
		                                    OnEntryNotFound::THROW_EXCEPTION);
		return table_entry;
	}
	case 1: {
		auto schema = catalog.GetDefaultSchema();
		auto table_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema, qualified_name[0],
		                                    OnEntryNotFound::THROW_EXCEPTION);
		return table_entry;
	}
	default:
		throw InvalidInputException("Too many identifiers in table name %s", input_string);
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
		EntryLookupInfo table_info(CatalogType::TABLE_ENTRY, qualified_name[2]);
		auto catalog_entry =
		    Catalog::GetEntry(context, qualified_name[0], qualified_name[1], table_info, OnEntryNotFound::RETURN_NULL);
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
			storage_location = table_entry.table_info.table_metadata.GetLatestMetadataJson();
			// Prepare Iceberg Scan from entry will create the secret needed to access the table
			table_entry.PrepareIcebergScanFromEntry(context);
			break;
		}
	} while (false);
	return storage_location;
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

	found = lpath.rfind("/data/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	throw InvalidConfigurationException("Could not create full path from Iceberg Path (%s) and the relative path (%s)",
	                                    iceberg_path, relative_file_path);
}

} // namespace duckdb
