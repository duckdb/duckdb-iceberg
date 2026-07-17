#include "core/metadata/puffin/iceberg_puffin_metadata.hpp"

#include "catalog/rest/api/catalog_utils.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/string_util.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

namespace {

constexpr data_t PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};
constexpr idx_t FOOTER_TRAILER_SIZE = sizeof(int32_t) + sizeof(uint32_t) + sizeof(PUFFIN_MAGIC);
constexpr idx_t FOOTER_FIXED_SIZE = sizeof(PUFFIN_MAGIC) + FOOTER_TRAILER_SIZE;

string ParseProperties(optional<case_insensitive_map_t<string>> &target, yyjson_val *properties_val) {
	if (!yyjson_is_obj(properties_val)) {
		return "properties is not an object";
	}
	case_insensitive_map_t<string> properties;
	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(properties_val, idx, max, key, val) {
		if (!yyjson_is_str(val)) {
			return StringUtil::Format("property value is not a string, found '%s' instead", yyjson_get_type_desc(val));
		}
		properties.emplace(yyjson_get_str(key), yyjson_get_str(val));
	}
	target = std::move(properties);
	return "";
}

string ParseBlobMetadata(IcebergPuffinBlobMetadata &result, yyjson_val *obj) {
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val || !yyjson_is_str(type_val)) {
		return "Puffin blob metadata property 'type' is missing or not a string";
	}
	result.type = yyjson_get_str(type_val);

	auto fields_val = yyjson_obj_get(obj, "fields");
	if (!fields_val || !yyjson_is_arr(fields_val)) {
		return "Puffin blob metadata property 'fields' is missing or not an array";
	}
	size_t idx, max;
	yyjson_val *val;
	yyjson_arr_foreach(fields_val, idx, max, val) {
		if (!yyjson_is_int(val)) {
			return StringUtil::Format("Puffin blob metadata field-id is not an integer, found '%s' instead",
			                          yyjson_get_type_desc(val));
		}
		result.fields.emplace_back(yyjson_get_int(val));
	}

	auto parse_required_int = [&](const char *key, int64_t &target) -> string {
		auto *int_val = yyjson_obj_get(obj, key);
		if (!int_val) {
			return StringUtil::Format("Puffin blob metadata property '%s' is missing", key);
		}
		if (yyjson_is_sint(int_val)) {
			target = yyjson_get_sint(int_val);
		} else if (yyjson_is_uint(int_val)) {
			target = yyjson_get_uint(int_val);
		} else {
			return StringUtil::Format("Puffin blob metadata property '%s' is not an integer, found '%s' instead", key,
			                          yyjson_get_type_desc(int_val));
		}
		return "";
	};

	auto error = parse_required_int("snapshot-id", result.snapshot_id);
	if (!error.empty()) {
		return error;
	}
	error = parse_required_int("sequence-number", result.sequence_number);
	if (!error.empty()) {
		return error;
	}
	error = parse_required_int("offset", result.offset);
	if (!error.empty()) {
		return error;
	}
	error = parse_required_int("length", result.length);
	if (!error.empty()) {
		return error;
	}

	auto compression_val = yyjson_obj_get(obj, "compression-codec");
	if (compression_val) {
		if (!yyjson_is_str(compression_val)) {
			return StringUtil::Format(
			    "Puffin blob metadata property 'compression-codec' is not a string, found '%s' instead",
			    yyjson_get_type_desc(compression_val));
		}
		result.compression_codec = string(yyjson_get_str(compression_val));
	}

	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		error = ParseProperties(result.properties, properties_val);
		if (!error.empty()) {
			return "Puffin blob metadata " + error;
		}
	}
	return "";
}

IcebergPuffinFileMetadata ParseFileMetadata(yyjson_val *root) {
	IcebergPuffinFileMetadata result;
	auto blobs_val = yyjson_obj_get(root, "blobs");
	if (!blobs_val || !yyjson_is_arr(blobs_val)) {
		throw InvalidInputException("Puffin file metadata property 'blobs' is missing or not an array");
	}
	size_t idx, max;
	yyjson_val *val;
	yyjson_arr_foreach(blobs_val, idx, max, val) {
		IcebergPuffinBlobMetadata blob;
		auto error = ParseBlobMetadata(blob, val);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		result.blobs.emplace_back(std::move(blob));
	}
	auto properties_val = yyjson_obj_get(root, "properties");
	if (properties_val) {
		auto error = ParseProperties(result.properties, properties_val);
		if (!error.empty()) {
			throw InvalidInputException("Puffin file metadata " + error);
		}
	}
	return result;
}

} // namespace

IcebergPuffinFileFooter IcebergPuffinReader::ReadFooter(FileSystem &fs, FileHandle &handle, const string &path,
                                                        optional<int64_t> expected_file_size,
                                                        optional<int64_t> expected_footer_size) {
	IcebergPuffinFileFooter result;

	auto file_size = handle.GetFileSize();
	if (expected_file_size && file_size != *expected_file_size) {
		throw InvalidConfigurationException("Puffin file '%s' size mismatch: expected %lld bytes, found %lld bytes",
		                                    path, *expected_file_size, file_size);
	}
	if (file_size < static_cast<int64_t>(sizeof(PUFFIN_MAGIC) + FOOTER_FIXED_SIZE)) {
		throw InvalidConfigurationException("Puffin file '%s' is too small to be valid", path);
	}

	auto trailer_buffer = Allocator::DefaultAllocator().Allocate(FOOTER_TRAILER_SIZE);
	auto trailer = trailer_buffer.get();
	fs.Read(handle, trailer, FOOTER_TRAILER_SIZE, file_size - FOOTER_TRAILER_SIZE);
	if (memcmp(trailer + sizeof(int32_t) + sizeof(uint32_t), PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC)) != 0) {
		throw InvalidConfigurationException("Puffin file '%s' has invalid trailing magic", path);
	}

	auto flags = Load<uint32_t>(trailer + sizeof(int32_t));
	if (flags & 0x1) {
		throw NotImplementedException("Puffin file '%s' uses a compressed footer payload, which is not supported",
		                              path);
	}

	auto footer_payload_size = Load<int32_t>(trailer);
	if (footer_payload_size < 0) {
		throw InvalidConfigurationException("Puffin file '%s' has a negative footer payload size", path);
	}
	result.footer_payload_size = static_cast<idx_t>(footer_payload_size);
	result.footer_size = result.footer_payload_size + FOOTER_FIXED_SIZE;

	if (expected_footer_size && result.footer_size != static_cast<idx_t>(*expected_footer_size)) {
		throw InvalidConfigurationException(
		    "Puffin file '%s' footer size mismatch: expected %lld bytes, found %llu bytes", path, *expected_footer_size,
		    result.footer_size);
	}
	if (result.footer_size > static_cast<idx_t>(file_size)) {
		throw InvalidConfigurationException("Puffin file '%s' footer extends past the end of the file", path);
	}

	auto footer_payload_start = file_size - FOOTER_TRAILER_SIZE - result.footer_payload_size;
	if (footer_payload_start < static_cast<int64_t>(sizeof(PUFFIN_MAGIC))) {
		throw InvalidConfigurationException("Puffin file '%s' has an invalid footer payload offset", path);
	}

	auto footer_buffer = Allocator::DefaultAllocator().Allocate(sizeof(PUFFIN_MAGIC) + result.footer_payload_size);
	auto footer = footer_buffer.get();
	fs.Read(handle, footer, sizeof(PUFFIN_MAGIC) + result.footer_payload_size,
	        footer_payload_start - static_cast<int64_t>(sizeof(PUFFIN_MAGIC)));
	if (memcmp(footer, PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC)) != 0) {
		throw InvalidConfigurationException("Puffin file '%s' has invalid footer leading magic", path);
	}

	auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
	    yyjson_read(reinterpret_cast<const char *>(footer + sizeof(PUFFIN_MAGIC)), result.footer_payload_size, 0));
	if (!doc) {
		throw InvalidInputException("Failed to parse Puffin footer JSON from '%s'", path);
	}
	result.file_metadata = ParseFileMetadata(yyjson_doc_get_root(doc.get()));
	return result;
}

IcebergPuffinFileFooter IcebergPuffinReader::ReadFooter(FileSystem &fs, const string &path,
                                                        optional<int64_t> expected_file_size,
                                                        optional<int64_t> expected_footer_size) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	return ReadFooter(fs, *handle, path, std::move(expected_file_size), std::move(expected_footer_size));
}

} // namespace duckdb
