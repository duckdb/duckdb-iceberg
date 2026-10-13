#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class FileSystem;

//! Deletion of files an operation leaves behind: the writes of a failed transaction, the files of a purged table. Like
//! Iceberg's CatalogUtil.dropTableData it is best effort: a file that cannot be deleted is logged, never an error, so
//! that as much as possible is deleted and the operation itself (a rollback, a DROP TABLE the catalog already
//! accepted) still completes.
class IcebergFileCleanup {
public:
	//! Delete 'paths' in one bulk request (httpfs batches them into S3 DeleteObjects) and log the outcome under
	//! 'operation': success in the Iceberg log ("Iceberg Transaction Cleanup, deleted 3 data_file(s)"), failure as a
	//! WARNING, since the operation itself succeeds and the log is the only place the user learns that files were
	//! left behind. A bulk delete reports one error for the whole batch, so some of the files may well be gone; the
	//! warning therefore names every path, because once the metadata that referenced them is deleted they are the
	//! only inventory left. The paths and the storage error are subject to 'redact_http_logs', like the HTTP logs.
	static void RemoveFilesBestEffort(ClientContext &context, FileSystem &fs, const vector<string> &paths,
	                                  const char *description, const char *operation);
};

} // namespace duckdb
