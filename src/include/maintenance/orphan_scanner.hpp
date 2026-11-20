#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class Connection;
class FileSystem;
struct FileMetadata;
struct IcebergTable;
struct SecretEntry;

class OrphanPathNormalizer {
public:
	explicit OrphanPathNormalizer(ClientContext &context, const string &scan_root = {});
	~OrphanPathNormalizer();

	//! Compare expanded paths conservatively across HTTPFS scheme aliases.
	string ReferenceKey(const string &path) const;
	//! Check S3 reference spelling, bucket and secret scope.
	void ValidateReferenceRoute(const string &path) const;
	const char *GetS3Scheme(const string &path) const;

private:
	void ValidateS3Routes(ClientContext &context, const string &scan_root);

	const char *s3_scan_scheme = nullptr;
	string s3_scan_bucket;
	unique_ptr<SecretEntry> s3_scan_secret;
	unordered_set<string> s3_aliases;
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
	vector<string> metadata_directories;
	unordered_set<string> files;
	//! Alternate spellings and reserved paths that do not contribute to the count.
	unordered_set<string> lookup_only;
	//! Device and file IDs identify paths that differ lexically.
	unordered_map<idx_t, unordered_set<idx_t>> local_file_ids;
};

struct OrphanFileVersion {
	string tag;
	int64_t size;
	timestamp_t last_modified;
};

//! A preview needs paths only. Deletion also needs the identity observed by the listing.
struct OrphanScanResult {
	vector<string> candidates;
	map<string, OrphanFileVersion> delete_candidates;
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

//! Check an expanded path, or just its leaf after a verified native listing.
void ValidateOrphanPath(const string &path, bool parent_verified = false);

//! Compare expanded paths, preserving the URI scheme used for I/O.
bool HasOrphanPathPrefix(const string &path, const string &prefix);

//! Return whether path contains a parent-directory traversal component.
bool HasParentDirectoryReference(const string &path);

} // namespace duckdb
