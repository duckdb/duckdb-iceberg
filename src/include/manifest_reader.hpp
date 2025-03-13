#pragma once

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Decoder.hh"
#include "avro/Encoder.hh"
#include "avro/Stream.hh"
#include "avro/ValidSchema.hh"
#include "iceberg_options.hpp"
#include "iceberg_types.hpp"

namespace duckdb {

// Manifest Reader

class ManifestReader {
public:
	ManifestReader() {
	}
	virtual ~ManifestReader() {
	}

public:
	bool Finished() const {
		return finished;
	}
	virtual unique_ptr<IcebergManifest> GetNext() = 0;

protected:
	bool finished = false;
};

class ManifestReaderV1 : public ManifestReader {
public:
	ManifestReaderV1(const string &table_path, const string &path, FileSystem &fs, const IcebergOptions &options) {
		auto file = options.allow_moved_paths ? IcebergUtils::GetFullPath(table_path, path, fs) : path;
		content = IcebergUtils::FileToString(file, fs);

		auto stream = avro::memoryInputStream((unsigned char *)content.c_str(), content.size());
		schema = avro::compileJsonSchemaFromString(MANIFEST_SCHEMA_V1);
		reader = make_uniq<avro::DataFileReader<c::manifest_file_v1>>(std::move(stream), schema);
	}

public:
	unique_ptr<IcebergManifest> GetNext() {
		if (finished || !reader->read(manifest_file)) {
			finished = true;
			return nullptr;
		}
		return make_uniq<IcebergManifest>(manifest_file);
	}

private:
	string content;
	avro::ValidSchema schema;
	unique_ptr<avro::DataFileReader<c::manifest_file_v1>> reader;
	c::manifest_file_v1 manifest_file;
};

class ManifestReaderV2 : public ManifestReader {
public:
	ManifestReaderV2(const string &table_path, const string &path, FileSystem &fs, const IcebergOptions &options) {
		auto file = options.allow_moved_paths ? IcebergUtils::GetFullPath(table_path, path, fs) : path;
		content = IcebergUtils::FileToString(file, fs);

		auto stream = avro::memoryInputStream((unsigned char *)content.c_str(), content.size());
		schema = avro::compileJsonSchemaFromString(MANIFEST_SCHEMA);
		reader = make_uniq<avro::DataFileReader<c::manifest_file>>(std::move(stream), schema);
	}

public:
	unique_ptr<IcebergManifest> GetNext() {
		// FIXME: use `hasMore` instead?
		if (finished || !reader->read(manifest_file)) {
			finished = true;
			return nullptr;
		}
		return make_uniq<IcebergManifest>(manifest_file);
	}

private:
	string content;
	avro::ValidSchema schema;
	unique_ptr<avro::DataFileReader<c::manifest_file>> reader;
	c::manifest_file manifest_file;
};

// Manifest Entry Reader

// FIXME: this is a little confusing, this is just used to initialize a ManifestEntryReader
// it does not hold any reading state
struct ManifestEntryReaderState {
public:
	ManifestEntryReaderState(IcebergManifest &manifest) : manifest(manifest), initialized(false), finished(false) {
	}
	ManifestEntryReaderState() : manifest(nullptr), initialized(false), finished(true) {
	}

public:
	optional_ptr<IcebergManifest> manifest;
	bool initialized = false;
	bool finished = false;
};

class ManifestEntryReader {
public:
	ManifestEntryReader(const string &table_path, FileSystem &fs, const IcebergOptions &options)
	    : table_path(table_path), fs(fs), options(options) {
	}
	virtual ~ManifestEntryReader() {
	}

public:
	ManifestEntryReaderState InitializeScan(IcebergManifest &manifest) {
		return ManifestEntryReaderState(manifest);
	}
	bool Finished() const {
		return finished;
	}
	virtual unique_ptr<IcebergManifestEntry> GetNext(ManifestEntryReaderState &state) = 0;

protected:
	string table_path;
	FileSystem &fs;
	const IcebergOptions &options;
	bool finished = false;
};

class ManifestEntryReaderV1 : public ManifestEntryReader {
public:
	ManifestEntryReaderV1(const string &table_path, const string &path, FileSystem &fs, const IcebergOptions &options)
	    : ManifestEntryReader(table_path, fs, options) {
	}

public:
	unique_ptr<IcebergManifestEntry> GetNext(ManifestEntryReaderState &state) {
		if (state.finished) {
			return nullptr;
		}

		if (!state.initialized) {
			// First call
			auto file = options.allow_moved_paths
			                ? IcebergUtils::GetFullPath(table_path, state.manifest->manifest_path, fs)
			                : state.manifest->manifest_path;
			content = IcebergUtils::FileToString(file, fs);

			auto stream = avro::memoryInputStream((unsigned char *)content.c_str(), content.size());
			schema = avro::compileJsonSchemaFromString(MANIFEST_ENTRY_SCHEMA_V1);
			reader = make_uniq<avro::DataFileReader<c::manifest_entry_v1>>(std::move(stream), schema);
			state.initialized = true;
		}

		if (!reader->read(manifest_entry)) {
			state.finished = true;
			return nullptr;
		}

		return make_uniq<IcebergManifestEntry>(manifest_entry);
	}

public:
	avro::ValidSchema schema;
	string content;
	c::manifest_entry_v1 manifest_entry;
	unique_ptr<avro::DataFileReader<c::manifest_entry_v1>> reader;
};

class ManifestEntryReaderV2 : public ManifestEntryReader {
public:
	ManifestEntryReaderV2(const string &table_path, const string &path, FileSystem &fs, const IcebergOptions &options)
	    : ManifestEntryReader(table_path, fs, options) {
	}

public:
	unique_ptr<IcebergManifestEntry> GetNext(ManifestEntryReaderState &state) {
		if (state.finished) {
			return nullptr;
		}

		if (!state.initialized) {
			// First call
			auto file = options.allow_moved_paths
			                ? IcebergUtils::GetFullPath(table_path, state.manifest->manifest_path, fs)
			                : state.manifest->manifest_path;
			content = IcebergUtils::FileToString(file, fs);

			auto stream = avro::memoryInputStream((unsigned char *)content.c_str(), content.size());
			schema = avro::compileJsonSchemaFromString(MANIFEST_ENTRY_SCHEMA);
			reader = make_uniq<avro::DataFileReader<manifest_entry>>(std::move(stream), schema);
			state.initialized = true;
		}

		if (!reader->read(current_entry)) {
			state.finished = true;
			return nullptr;
		}

		return make_uniq<IcebergManifestEntry>(current_entry);
	}

public:
	avro::ValidSchema schema;
	string content;
	manifest_entry current_entry;
	unique_ptr<avro::DataFileReader<manifest_entry>> reader;
};

} // namespace duckdb
