#include "duckdb.hpp"
#include "iceberg_utils.hpp"
#include "fstream"
#include "duckdb/common/gzip_file_system.hpp"

namespace duckdb {

idx_t IcebergUtils::CountOccurrences(const string &input, const string &to_find) {
	size_t pos = input.find(to_find);
	idx_t count = 0;
	while (pos != std::string::npos) {
		pos = input.find(to_find, pos + to_find.length()); // move past current match
		count++;
	}
	return count;
}

string IcebergUtils::FileToString(const string &path, FileSystem &fs) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = handle->GetFileSize();
	string ret_val(file_size, ' ');
	handle->Read((char *)ret_val.c_str(), file_size);
	return ret_val;
}

// Function to decompress a gz file content string
string IcebergUtils::GzFileToString(const string &path, FileSystem &fs) {
	// Initialize zlib variables
	string gzipped_string = FileToString(path, fs);
	return GZipFileSystem::UncompressGZIPString(gzipped_string);
}

string IcebergUtils::GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs) {
	std::size_t found = relative_file_path.rfind("/metadata/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	found = relative_file_path.rfind("/data/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	throw InvalidConfigurationException("Did not recognize iceberg path");
}

uint64_t IcebergUtils::TryGetNumFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_type(val) != YYJSON_TYPE_NUM) {
		throw InvalidConfigurationException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_uint(val);
}

bool IcebergUtils::TryGetBoolFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_type(val) != YYJSON_TYPE_BOOL) {
		throw InvalidConfigurationException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_bool(val);
}

string IcebergUtils::TryGetStrFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_type(val) != YYJSON_TYPE_STR) {
		throw InvalidConfigurationException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_str(val);
}

template <class TYPE, uint8_t TYPE_NUM, TYPE (*get_function)(yyjson_val *obj)>
static TYPE TemplatedTryGetYYJson(yyjson_val *obj, const string &field, TYPE default_val, bool fail_on_missing = true) {
	auto val = yyjson_obj_get(obj, field.c_str());
	if (val && yyjson_get_type(val) == TYPE_NUM) {
		return get_function(val);
	} else if (!fail_on_missing) {
		return default_val;
	}
	throw InvalidConfigurationException("Invalid field found while parsing field: " + field);
}

uint64_t IcebergUtils::TryGetNumFromObject(yyjson_val *obj, const string &field, bool fail_on_missing,
                                           uint64_t default_val) {
	return TemplatedTryGetYYJson<uint64_t, YYJSON_TYPE_NUM, yyjson_get_uint>(obj, field, default_val, fail_on_missing);
}
bool IcebergUtils::TryGetBoolFromObject(yyjson_val *obj, const string &field, bool fail_on_missing, bool default_val) {
	return TemplatedTryGetYYJson<bool, YYJSON_TYPE_BOOL, yyjson_get_bool>(obj, field, default_val, fail_on_missing);
}
string IcebergUtils::TryGetStrFromObject(yyjson_val *obj, const string &field, bool fail_on_missing,
                                         const char *default_val) {
	return TemplatedTryGetYYJson<const char *, YYJSON_TYPE_STR, yyjson_get_str>(obj, field, default_val,
	                                                                            fail_on_missing);
}

} // namespace duckdb
