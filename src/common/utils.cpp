#include "duckdb.hpp"
#include "iceberg_utils.hpp"
#include "fstream"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_table_entry.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "storage/irc_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

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
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = handle->GetFileSize();
	string ret_val(file_size, ' ');
	// We need to iterate, given Read() might return less bytes than expected
	uint64_t bytes_read = 0;
	while (bytes_read < file_size) {
		int64_t r = handle->Read((char *)ret_val.c_str() + bytes_read, file_size - bytes_read);
		if (r == 0) {
			throw IOException("Could not Read all bytes from the file");
		}
		bytes_read += r;
	}
	return ret_val;
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
			auto &table_entry = catalog_entry->Cast<ICTableEntry>();
			storage_location = table_entry.PrepareIcebergScanFromEntry(context);
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
