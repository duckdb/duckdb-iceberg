//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/remote_signing/iceberg_remote_signed_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

#include "storage/remote_signing/iceberg_remote_signing.hpp"

namespace duckdb {

class IcebergRemoteSignedFileSystem;

class IcebergRemoteSignedFileHandle : public FileHandle {
public:
	IcebergRemoteSignedFileHandle(IcebergRemoteSignedFileSystem &fs, const OpenFileInfo &file, FileOpenFlags flags,
	                              IcebergRemoteSigningTarget target, string http_url, string host,
	                              weak_ptr<ClientContext> context);

public:
	void Close() override {
	}

public:
	IcebergRemoteSigningTarget target;
	string http_url;
	string host;
	weak_ptr<ClientContext> context;
	idx_t length = 0;
	idx_t file_offset = 0;
	timestamp_t last_modified;
	string etag;
};

//! Reads Iceberg data below a table location whose REST catalog delegates storage access through
//! remote request signing instead of vending credentials
class IcebergRemoteSignedFileSystem : public FileSystem {
public:
	static constexpr const char *NAME = "IcebergRemoteSignedFileSystem";

public:
	explicit IcebergRemoteSignedFileSystem(shared_ptr<IcebergRemoteSigningRegistry> registry_p)
	    : registry(std::move(registry_p)) {
	}

public:
	string GetName() const override {
		return NAME;
	}
	bool CanHandleFile(const string &fpath) override;
	//! Take priority over httpfs for the table locations that require remote signing, independent of
	//! the order in which the two extensions were loaded
	bool IsManuallySet() override {
		return true;
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	FileMetadata Stats(FileHandle &handle) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	void Reset(FileHandle &handle) override;
	string PathSeparator(const string &path) override {
		return "/";
	}

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
	bool SupportsOpenFileExtended() const override {
		return true;
	}

private:
	shared_ptr<IcebergRemoteSigningRegistry> registry;
};

} // namespace duckdb
