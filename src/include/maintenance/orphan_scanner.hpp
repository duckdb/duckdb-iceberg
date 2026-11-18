#pragma once

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class Connection;
struct IcebergTable;
struct SecretEntry;

class OrphanPathNormalizer {
public:
	explicit OrphanPathNormalizer(ClientContext &context, const string &scan_root = {},
	                              optional_ptr<ClientContext> caller = nullptr);
	~OrphanPathNormalizer();

	//! Compare expanded paths conservatively across HTTPFS scheme aliases.
	string ReferenceKey(const string &path, bool validate_file_reference = false) const;
	const char *GetS3Scheme(const string &path) const;

private:
	void ValidateS3Routes(const string &scan_root);
	void ValidateReferenceRoute(const string &path, const char *scheme) const;
	unique_ptr<SecretEntry> ResolveStorageSecret(const string &path) const;
	void RequireSameS3Route(const string &path) const;

	ClientContext &context;
	string s3_scan_prefix;
	const char *s3_scan_scheme = nullptr;
	string s3_scan_bucket;
	unique_ptr<SecretEntry> s3_scan_secret;
	unordered_set<string> s3_aliases;
	optional<CatalogTransaction> caller_catalog;
};

//! Check whether an expanded path uses the local filesystem.
bool IsLocalOrphanPath(const string &path);

//! Join a known child to an expanded location, preserving literal object keys.
string JoinOrphanPath(FileSystem &fs, const string &directory, const string &child);

//! Require an absolute path or a storage URI after file-system expansion.
void RequireAbsoluteOrphanPath(const string &path, const char *kind);

struct OrphanFileReferences {
	explicit OrphanFileReferences(const string &scan_key);
	void AddMetadataDirectory(string key);
	bool Add(const string &path, string key, bool count = true);
	bool Contains(const string &key) const;
	idx_t Count() const {
		return files.size();
	}

	void AddLocalFile(FileSystem &fs, const string &path);
	bool ContainsLocalFile(const FileMetadata &metadata, const string &path) const;

private:
	string ReferenceAlias(const string &key) const;
	string scan_scheme_prefix;
	unordered_set<string> metadata_directories;
	unordered_set<string> files;
	//! Alternate spellings and reserved paths that do not contribute to the count.
	unordered_set<string> lookup_only;
	//! Device and file IDs identify paths that differ lexically.
	unordered_map<idx_t, unordered_set<idx_t>> local_file_ids;
};

struct OrphanScanResult {
	vector<string> candidates;
	//! Delete only candidates observed exactly once in the listing.
	unordered_map<string, idx_t> delete_candidates;
	int64_t scanned_count = 0;
};

//! File I/O for a cleanup must not modify the calling connection's settings.
Connection OpenOrphanStorage(ClientContext &caller, const IcebergTable &table, const string &root,
                             const string &metadata_location, bool for_deletion);

//! Input for the storage listing phase of iceberg_remove_orphan_files.
struct OrphanScannerInput {
	ClientContext &context;
	const OrphanPathNormalizer &paths;
	const OrphanFileReferences &referenced_files;
	const string &scan_root;
	timestamp_t older_than;
	bool for_deletion;
};

//! List the scan root and find files that are old enough and unreferenced.
OrphanScanResult FindOrphanFiles(const OrphanScannerInput &input);

//! Check an expanded path for traversal and local symbolic links.
void ValidateOrphanPath(const string &path);

//! Compare expanded paths, preserving the URI scheme used for I/O.
bool HasOrphanPathPrefix(const string &path, const string &prefix);

//! Return whether path contains a parent-directory traversal component.
bool HasParentDirectoryReference(const string &path);

} // namespace duckdb
