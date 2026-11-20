#include "catalog/rest/iceberg_table_metadata_cache.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_file_credentials.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_rest_backend.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

static IcebergTableAccess GetLoadTableAccess(const rest_api_objects::LoadTableResult &result) {
	IcebergTableAccess access;
	if (result.config) {
		access.config = *result.config;
	}
	if (result.storage_credentials) {
		access.storage_credentials = IcebergRestBackend::ToStorageCredentials(*result.storage_credentials);
	}
	return access;
}

IcebergLoadTableResponse::IcebergLoadTableResponse(rest_api_objects::LoadTableResult result_p)
    : IcebergLoadedTable(GetLoadTableAccess(result_p)), result(std::move(result_p)) {
}

shared_ptr<const IcebergTableMetadata> IcebergLoadTableResponse::GetMetadata(ClientContext &context,
                                                                             const IcebergTable &table) const {
	// Resolve the callback on every initialization, even when the file's parsed contents are already cached.
	// Registration and authorization remain connection/transaction-specific.
	optional<OpenFileInfo> metadata_file;
	const bool use_metadata_file =
	    IcebergRestBackend::Get(table.catalog).options.use_metadata_location && result.metadata_location;
	auto log_fallback = [&](const std::exception &ex) {
		DUCKDB_LOG(context, IcebergLogType,
		           "Could not load table metadata from metadata-location '%s': %s - falling back to the "
		           "metadata embedded in the catalog response",
		           *result.metadata_location, ex.what());
	};
	if (use_metadata_file) {
		try {
			metadata_file = IcebergFileCredentialsCallback::GetFileInfo(context, table, *result.metadata_location);
		} catch (std::exception &ex) {
			log_fallback(ex);
		}
	}
	lock_guard<mutex> guard(lock);
	if (metadata && (!use_metadata_file || metadata_file)) {
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg metadata phase=table_metadata_cache scope=catalog_initialization parsed_metadata_hits=1 "
		           "table_metadata_conversions=0 snapshots_converted=0");
		return metadata;
	}
	unique_ptr<rest_api_objects::TableMetadata> file_metadata;
	if (metadata_file) {
		try {
			auto &fs = FileSystem::GetFileSystem(context);
			auto caching_fs = make_shared_ptr<CachingFileSystemWrapper>(fs, *context.db);
			file_metadata = make_uniq<rest_api_objects::TableMetadata>(
			    IcebergTableMetadata::Parse(*metadata_file, *caching_fs, "none"));
		} catch (std::exception &ex) {
			log_fallback(ex);
		}
	}
	auto parsed = make_shared_ptr<const IcebergTableMetadata>(
	    IcebergTableMetadata::FromTableMetadata(file_metadata ? *file_metadata : result.metadata));
	DUCKDB_LOG(context, IcebergLogType,
	           "Iceberg metadata phase=table_metadata_conversion scope=catalog_initialization "
	           "table_metadata_conversions=1 snapshots_converted=%llu",
	           parsed->snapshots.size());
	// A transient file failure does not establish the effective authoritative version. Retry on the next
	// read, and never substitute this fallback for a successfully parsed metadata-location payload.
	if (!use_metadata_file || file_metadata) {
		metadata = parsed;
	}
	return parsed;
}

} // namespace duckdb
