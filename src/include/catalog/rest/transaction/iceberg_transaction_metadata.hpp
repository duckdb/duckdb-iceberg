#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "core/metadata/iceberg_file_identity.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct VersionedIcebergManifestDeletes;

struct IcebergManifestDeletes {
public:
	void InvalidateFile(const IcebergFileIdentity &file) {
		data_files.emplace(file, optional_idx());
	}
	bool IsInvalidated(const IcebergFileIdentity &file) const {
		return data_files.count(file);
	}
	VersionedIcebergManifestDeletes AtVersion(idx_t alter_version);
	bool IsEmpty() const {
		return data_files.empty();
	}

private:
	idx_t Merge(IcebergManifestDeletes &&other, idx_t alter_version) {
		idx_t added_count = 0;
		for (auto &entry : other.data_files) {
			added_count += data_files.emplace(entry.first, alter_version).second;
		}
		return added_count;
	}
	bool IsInvalidatedAt(const IcebergFileIdentity &file, idx_t alter_version) const {
		auto entry = data_files.find(file);
		return entry != data_files.end() && entry->second.IsValid() && entry->second.GetIndex() == alter_version;
	}

private:
	friend struct VersionedIcebergManifestDeletes;

	//! File identities tagged with the alter that invalidated them.
	unordered_map<IcebergFileIdentity, optional_idx, IcebergFileIdentityHash> data_files;
};

struct VersionedIcebergManifestDeletes {
public:
	VersionedIcebergManifestDeletes(IcebergManifestDeletes &manifest_deletes_p, idx_t alter_version_p)
	    : manifest_deletes(manifest_deletes_p), alter_version(alter_version_p) {
	}

	idx_t Merge(IcebergManifestDeletes &&other) {
		return manifest_deletes.Merge(std::move(other), alter_version);
	}
	bool IsInvalidated(const IcebergFileIdentity &file) const {
		return manifest_deletes.IsInvalidatedAt(file, alter_version);
	}

private:
	IcebergManifestDeletes &manifest_deletes;
	idx_t alter_version;
};

inline VersionedIcebergManifestDeletes IcebergManifestDeletes::AtVersion(idx_t alter_version) {
	return VersionedIcebergManifestDeletes(*this, alter_version);
}

} // namespace duckdb
