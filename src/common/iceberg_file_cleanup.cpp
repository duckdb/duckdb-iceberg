#include "common/iceberg_file_cleanup.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include "iceberg_logging.hpp"

namespace duckdb {

void IcebergFileCleanup::RemoveFilesBestEffort(ClientContext &context, FileSystem &fs, const vector<string> &paths,
                                               const char *description, const char *operation) {
	if (paths.empty()) {
		return;
	}
	//! TODO: FileSystem::RemoveFiles stops at the first failure (the default implementation at the first file, httpfs's
	//! S3 implementation at the first DeleteObjects batch with a per-key error, skipping the remaining batches), so one
	//! undeletable file leaves the rest behind. Until httpfs finishes every batch and reports the failed keys like
	//! Iceberg's S3FileIO, chunk the paths here and retry a failed chunk file by file.
	try {
		fs.RemoveFiles(paths);
		DUCKDB_LOG(context, IcebergLogType, "%s, deleted %llu %s(s)", operation, static_cast<uint64_t>(paths.size()),
		           description);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "%s, failed to delete one or more of %llu %s(s) [%s]: %s", operation,
		                   static_cast<uint64_t>(paths.size()), description, IcebergLogType::Redact(context, paths),
		                   IcebergLogType::Redact(context, error.RawMessage()));
	}
}

} // namespace duckdb
