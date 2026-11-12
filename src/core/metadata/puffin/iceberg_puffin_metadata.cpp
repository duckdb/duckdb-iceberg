#include "core/metadata/puffin/iceberg_puffin_metadata.hpp"

#include "catalog/rest/api/catalog_utils.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb/common/json_document.hpp"

namespace duckdb {

namespace {

constexpr data_t PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};
constexpr idx_t FOOTER_TRAILER_SIZE = sizeof(int32_t) + sizeof(uint32_t) + sizeof(PUFFIN_MAGIC);
constexpr idx_t FOOTER_FIXED_SIZE = sizeof(PUFFIN_MAGIC) + FOOTER_TRAILER_SIZE;

JSONMutableValue WriteBlobMetadata(JSONWriter &json, const IcebergPuffinBlobMetadata &blob) {
	auto obj = json.CreateObject();
	obj.AddString("type", blob.type);
	auto fields = json.CreateArray();
	obj.Add("fields", fields);
	for (auto field : blob.fields) {
		fields.Append(json.CreateSignedInteger(field));
	}
	obj.Add("snapshot-id", json.CreateSignedInteger(blob.snapshot_id));
	obj.Add("sequence-number", json.CreateSignedInteger(blob.sequence_number));
	obj.Add("offset", json.CreateSignedInteger(blob.offset));
	obj.Add("length", json.CreateSignedInteger(blob.length));
	if (blob.compression_codec) {
		obj.AddString("compression-codec", *blob.compression_codec);
	}
	if (blob.properties) {
		auto properties = json.CreateObject();
		obj.Add("properties", properties);
		for (const auto &property : *blob.properties) {
			properties.AddString(property.first, property.second);
		}
	}
	return obj;
}

string ParseProperties(optional<case_insensitive_map_t<string>> &target, JSONValue properties_val) {
	if (!properties_val.IsObject()) {
		return "properties is not an object";
	}
	case_insensitive_map_t<string> properties;
	string error;
	properties_val.IterateObject([&](const string &key, JSONValue val) {
		if (!val.IsString()) {
			error = "property value is not a string";
			return;
		}
		properties.emplace(key, val.GetString());
	});
	if (!error.empty()) {
		return error;
	}
	target = std::move(properties);
	return "";
}

string ParseBlobMetadata(IcebergPuffinBlobMetadata &result, JSONValue obj) {
	auto type_val = obj.GetMember("type");
	if (!type_val.IsString()) {
		return "Puffin blob metadata property 'type' is missing or not a string";
	}
	result.type = type_val.GetString();

	auto fields_val = obj.GetMember("fields");
	if (!fields_val.IsArray()) {
		return "Puffin blob metadata property 'fields' is missing or not an array";
	}
	string fields_error;
	fields_val.IterateArray([&](JSONValue val) {
		if (!val.IsInteger()) {
			fields_error = "Puffin blob metadata field-id is not an integer";
			return;
		}
		result.fields.emplace_back(val.GetType() == JSONValueType::SIGNED_INTEGER ? val.GetSignedInteger()
		                                                                          : val.GetUnsignedInteger());
	});
	if (!fields_error.empty()) {
		return fields_error;
	}

	auto parse_required_int = [&](const char *key, int64_t &target) -> string {
		auto int_val = obj.GetMember(key);
		if (!int_val.IsValid()) {
			return StringUtil::Format("Puffin blob metadata property '%s' is missing", key);
		}
		if (int_val.GetType() == JSONValueType::SIGNED_INTEGER) {
			target = int_val.GetSignedInteger();
		} else if (int_val.GetType() == JSONValueType::UNSIGNED_INTEGER) {
			target = static_cast<int64_t>(int_val.GetUnsignedInteger());
		} else {
			return StringUtil::Format("Puffin blob metadata property '%s' is not an integer", key);
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

	auto compression_val = obj.GetMember("compression-codec");
	if (compression_val.IsValid()) {
		if (!compression_val.IsString()) {
			return "Puffin blob metadata property 'compression-codec' is not a string";
		}
		result.compression_codec = compression_val.GetString();
	}

	auto properties_val = obj.GetMember("properties");
	if (properties_val.IsValid()) {
		error = ParseProperties(result.properties, properties_val);
		if (!error.empty()) {
			return "Puffin blob metadata " + error;
		}
	}
	return "";
}

using IcebergPuffinFileMetadataResult = std::variant<IcebergPuffinFileMetadata, string>;

IcebergPuffinFileMetadataResult ParseFileMetadata(JSONValue root) {
	IcebergPuffinFileMetadata result;
	auto blobs_val = root.GetMember("blobs");
	if (!blobs_val.IsArray()) {
		return "Puffin file metadata property 'blobs' is missing or not an array";
	}
	string blobs_error;
	blobs_val.IterateArray([&](JSONValue val) {
		if (!blobs_error.empty()) {
			return;
		}
		IcebergPuffinBlobMetadata blob;
		auto error = ParseBlobMetadata(blob, val);
		if (!error.empty()) {
			blobs_error = std::move(error);
			return;
		}
		result.blobs.emplace_back(std::move(blob));
	});
	if (!blobs_error.empty()) {
		return blobs_error;
	}
	auto properties_val = root.GetMember("properties");
	if (properties_val.IsValid()) {
		auto error = ParseProperties(result.properties, properties_val);
		if (!error.empty()) {
			return "Puffin file metadata " + error;
		}
	}
	return result;
}

} // namespace

IcebergPuffinFileLayout::IcebergPuffinFileLayout()
    : data_size(sizeof(PUFFIN_MAGIC)), footer_size(FOOTER_FIXED_SIZE + strlen("{\"blobs\":[]}")) {
}

idx_t IcebergPuffinFileLayout::AddBlob(idx_t size, IcebergPuffinBlobMetadata metadata) {
	metadata.offset = NumericCast<int64_t>(data_size);
	metadata.length = NumericCast<int64_t>(size);
	JSONWriter json;
	json.SetRoot(WriteBlobMetadata(json, metadata));
	footer_size += json.ToString().size() + (blob_count++ > 0 ? 1 : 0);
	data_size += size;
	return data_size + footer_size;
}

IcebergPuffinWriter::IcebergPuffinWriter(FileSystem &fs, const string &path)
    : writer(fs, path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE) {
	writer.WriteData(PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC));
}

idx_t IcebergPuffinWriter::WriteBlob(const vector<data_t> &blob, IcebergPuffinBlobMetadata metadata) {
	auto offset = writer.GetTotalWritten();
	metadata.offset = NumericCast<int64_t>(offset);
	metadata.length = NumericCast<int64_t>(blob.size());
	writer.WriteData(blob.data(), blob.size());
	blobs.push_back(std::move(metadata));
	return offset;
}

idx_t IcebergPuffinWriter::Finish() {
	JSONWriter json;
	auto root = json.CreateObject();
	json.SetRoot(root);
	auto blob_array = json.CreateArray();
	root.Add("blobs", blob_array);
	for (const auto &blob : blobs) {
		blob_array.Append(WriteBlobMetadata(json, blob));
	}
	auto payload = json.ToString();
	// Footer: magic, uncompressed JSON, little-endian payload size, flags, magic.
	data_t trailer[FOOTER_TRAILER_SIZE];
	Store<int32_t>(NumericCast<int32_t>(payload.size()), trailer);
	Store<uint32_t>(0, trailer + sizeof(int32_t));
	memcpy(trailer + sizeof(int32_t) + sizeof(uint32_t), PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC));
	writer.WriteData(PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC));
	writer.WriteData(reinterpret_cast<const_data_ptr_t>(payload.data()), payload.size());
	writer.WriteData(trailer, sizeof(trailer));
	writer.Close();
	return writer.GetTotalWritten();
}

IcebergPuffinFileFooterResult IcebergPuffinReader::ReadFooter(FileSystem &fs, FileHandle &handle, const string &path,
                                                              optional<int64_t> expected_file_size,
                                                              optional<int64_t> expected_footer_size) {
	IcebergPuffinFileFooter result;

	auto file_size = handle.GetFileSize();
	if (expected_file_size && file_size != *expected_file_size) {
		return StringUtil::Format("Puffin file '%s' size mismatch: expected %lld bytes, found %lld bytes", path,
		                          *expected_file_size, file_size);
	}
	if (file_size < static_cast<int64_t>(sizeof(PUFFIN_MAGIC) + FOOTER_FIXED_SIZE)) {
		return StringUtil::Format("Puffin file '%s' is too small to be valid", path);
	}

	auto trailer_buffer = Allocator::DefaultAllocator().Allocate(FOOTER_TRAILER_SIZE);
	auto trailer = trailer_buffer.get();
	fs.Read(handle, trailer, FOOTER_TRAILER_SIZE, file_size - FOOTER_TRAILER_SIZE);
	if (memcmp(trailer + sizeof(int32_t) + sizeof(uint32_t), PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC)) != 0) {
		return StringUtil::Format("Puffin file '%s' has invalid trailing magic", path);
	}

	auto flags = Load<uint32_t>(trailer + sizeof(int32_t));
	if (flags & 0x1) {
		return StringUtil::Format("Puffin file '%s' uses a compressed footer payload, which is not supported", path);
	}

	auto footer_payload_size = Load<int32_t>(trailer);
	if (footer_payload_size < 0) {
		return StringUtil::Format("Puffin file '%s' has a negative footer payload size", path);
	}
	result.footer_payload_size = static_cast<idx_t>(footer_payload_size);
	result.footer_size = result.footer_payload_size + FOOTER_FIXED_SIZE;

	if (expected_footer_size && result.footer_size != static_cast<idx_t>(*expected_footer_size)) {
		return StringUtil::Format("Puffin file '%s' footer size mismatch: expected %lld bytes, found %llu bytes", path,
		                          *expected_footer_size, result.footer_size);
	}
	if (result.footer_size > static_cast<idx_t>(file_size)) {
		return StringUtil::Format("Puffin file '%s' footer extends past the end of the file", path);
	}

	auto footer_payload_start = file_size - FOOTER_TRAILER_SIZE - result.footer_payload_size;
	if (footer_payload_start < static_cast<int64_t>(sizeof(PUFFIN_MAGIC))) {
		return StringUtil::Format("Puffin file '%s' has an invalid footer payload offset", path);
	}

	auto footer_buffer = Allocator::DefaultAllocator().Allocate(sizeof(PUFFIN_MAGIC) + result.footer_payload_size);
	auto footer = footer_buffer.get();
	fs.Read(handle, footer, sizeof(PUFFIN_MAGIC) + result.footer_payload_size,
	        footer_payload_start - static_cast<int64_t>(sizeof(PUFFIN_MAGIC)));
	if (memcmp(footer, PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC)) != 0) {
		return StringUtil::Format("Puffin file '%s' has invalid footer leading magic", path);
	}

	JSONParseError parse_error;
	auto doc = JSONDocument::TryParse(reinterpret_cast<const char *>(footer + sizeof(PUFFIN_MAGIC)),
	                                  result.footer_payload_size, parse_error);
	if (!doc) {
		return StringUtil::Format("Puffin file '%s' has invalid footer JSON at byte %llu: %s", path,
		                          parse_error.position, parse_error.message);
	}
	auto metadata_result = ParseFileMetadata(doc->GetRoot());
	if (auto error = std::get_if<string>(&metadata_result)) {
		return StringUtil::Format("Puffin file '%s' has invalid metadata: %s", path, *error);
	}
	result.file_metadata = std::get<IcebergPuffinFileMetadata>(std::move(metadata_result));
	return result;
}

IcebergPuffinFileFooterResult IcebergPuffinReader::ReadFooter(FileSystem &fs, const string &path,
                                                              optional<int64_t> expected_file_size,
                                                              optional<int64_t> expected_footer_size) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	return ReadFooter(fs, *handle, path, std::move(expected_file_size), std::move(expected_footer_size));
}

} // namespace duckdb
