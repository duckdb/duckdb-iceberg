#include "core/deletes/iceberg_deletion_vector.hpp"

#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/common/bswap.hpp"

#include "planning/iceberg_multi_file_list.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/api/catalog_utils.hpp"

namespace duckdb {

namespace {

class CRC32 {
public:
	CRC32() : crc(0xFFFFFFFF) {
		InitTable();
	}

public:
	static void InitTable() {
		if (table_initialized)
			return;

		for (uint32_t i = 0; i < 256; i++) {
			uint32_t c = i;
			for (int j = 0; j < 8; j++) {
				if (c & 1) {
					c = 0xEDB88320 ^ (c >> 1);
				} else {
					c = c >> 1;
				}
			}
			crc_table[i] = c;
		}
		table_initialized = true;
	}

public:
	void Update(const data_t *data, idx_t length) {
		for (idx_t i = 0; i < length; i++) {
			crc = crc_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
		}
	}

	void Update(const vector<data_t> &data) {
		Update(data.data(), data.size());
	}

	uint32_t GetValue() const {
		return crc ^ 0xFFFFFFFF;
	}

	void Reset() {
		crc = 0xFFFFFFFF;
	}

private:
	uint32_t crc;
	static uint32_t crc_table[256];
	static bool table_initialized;
};

uint32_t CRC32::crc_table[256];
bool CRC32::table_initialized = false;

} // namespace

shared_ptr<IcebergDeletionVectorData> IcebergDeletionVectorData::FromBlob(const BoundIcebergManifestEntry &entry,
                                                                          data_ptr_t blob_start, idx_t blob_length) {
	//! https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type

	auto blob_end = blob_start + blob_length;
	auto vector_size = Load<uint32_t>(blob_start);
	vector_size = BSwap(vector_size);
	blob_start += sizeof(uint32_t);
	D_ASSERT(blob_start < blob_end);

	if (blob_length < 12) {
		throw InvalidConfigurationException("Blob is too small (length of %d bytes) to be a deletion-vector-v1",
		                                    blob_length);
	}
	constexpr char DELETION_VECTOR_MAGIC[] = {'\xD1', '\xD3', '\x39', '\x64'};
	char magic_bytes[4];

	auto checksummed_data_start = blob_start;
	memcpy(magic_bytes, blob_start, 4);
	blob_start += 4;
	vector_size -= 4;
	D_ASSERT(blob_start < blob_end);

	auto memcmp_res = memcmp(DELETION_VECTOR_MAGIC, magic_bytes, 4);
	if (memcmp_res != 0) {
		throw InvalidInputException("Magic bytes mismatch, deletion vector is corrupt!");
	}

	int64_t amount_of_bitmaps = Load<int64_t>(blob_start);
	blob_start += sizeof(int64_t);
	vector_size -= sizeof(int64_t);
	D_ASSERT(blob_start < blob_end);

	auto result_p = make_shared_ptr<IcebergDeletionVectorData>(entry);
	auto &result = *result_p;
	result.bitmaps.reserve(amount_of_bitmaps);
	for (int64_t i = 0; i < amount_of_bitmaps; i++) {
		auto key = Load<int32_t>(blob_start);
		blob_start += sizeof(int32_t);
		vector_size -= sizeof(int32_t);
		D_ASSERT(blob_start < blob_end);

		size_t bitmap_size =
		    roaring::api::roaring_bitmap_portable_deserialize_size((const char *)blob_start, vector_size);
		auto bitmap = roaring::Roaring::readSafe((const char *)blob_start, bitmap_size);
		blob_start += bitmap_size;
		vector_size -= bitmap_size;
		D_ASSERT(blob_start < blob_end);
		result.bitmaps.emplace(key, std::move(bitmap));
	}
	//! Compute and compare the checksum
	auto checksummed_data_length = blob_start - checksummed_data_start;
	auto stored_checksum = BSwap(Load<uint32_t>(blob_start));
	blob_start += sizeof(uint32_t);
	D_ASSERT(blob_start == blob_end);

	CRC32 crc;
	crc.Update(checksummed_data_start, checksummed_data_length);
	uint32_t checksum = crc.GetValue();
	if (checksum != stored_checksum) {
		throw InvalidInputException(
		    "Stored checksum (%d) does not match computed checksum (%d), the DeletionVector is corrupted",
		    stored_checksum, checksum);
	}
	return result_p;
}

void IcebergMultiFileList::ScanPuffinFile(const BoundIcebergManifestEntry &bound_entry) const {
	auto &entry = bound_entry.entry;
	auto &data_file = entry.data_file;
	auto &table_metadata = GetMetadata();
	auto iceberg_version = table_metadata.iceberg_version;
	if (iceberg_version < 3) {
		throw InvalidConfigurationException("DeletionVector not supported in Iceberg V%d", iceberg_version);
	}
	auto file_path = data_file.file_path;
	D_ASSERT(!data_file.referenced_data_file.empty());

	auto caching_file_system = CachingFileSystem::Get(context);

	auto caching_file_handle = caching_file_system.OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ);
	data_ptr_t data = nullptr;

	D_ASSERT(!data_file.content_offset.IsNull());
	D_ASSERT(!data_file.content_size_in_bytes.IsNull());

	auto offset = data_file.content_offset.GetValue<int64_t>();
	auto length = data_file.content_size_in_bytes.GetValue<int64_t>();

	auto buf_handle = caching_file_handle->Read(data, length, offset);
	auto buffer_data = buf_handle.Ptr();

	auto it = shared_state->positional_delete_data.find(data_file.referenced_data_file);
	if (it != shared_state->positional_delete_data.end()) {
		//! Another delete already exists for this table
		auto &existing_delete = *it->second;
		if (existing_delete.type == IcebergDeleteType::DELETION_VECTOR) {
			throw InvalidConfigurationException(
			    "Table is corrupt, two or more deletion vectors exist for the same referenced_data_file");
		}
	}
	//! NOTE: assign, don't emplace, deletion vectors take priority over any remaining positional delete files
	shared_state->positional_delete_data[data_file.referenced_data_file] =
	    IcebergDeletionVectorData::FromBlob(bound_entry, buffer_data, length);
}

idx_t IcebergDeletionVector::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	if (count == 0) {
		return 0;
	}
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t selection_idx = 0;

	auto &bitmaps = data->bitmaps;
	idx_t offset = 0;
	while (offset < count) {
		const row_t current_row = start_row_index + offset;
		const int32_t high = static_cast<int32_t>(current_row >> 32);

		const row_t next_high_boundary = ((static_cast<row_t>(high) + 1) << 32);
		//! FIXME: How do we test this? These offsets are **huge**
		const idx_t next_offset = MinValue<idx_t>(start_row_index + count, next_high_boundary) - start_row_index;

		lock_guard<mutex> guard(lock);
		//! Update the state
		if (!has_current_high || current_high != high) {
			auto it = bitmaps.find(high);
			if (it == bitmaps.end()) {
				current_bitmap = nullptr;
			} else {
				current_bitmap = it->second;
				bulk_context = roaring::BulkContext();
			}
			current_high = high;
			has_current_high = true;
		}

		if (!current_bitmap) {
			for (idx_t i = offset; i < next_offset; ++i) {
				result_sel.set_index(selection_idx++, i);
			}
		} else {
			const roaring::Roaring &bitmap = *current_bitmap;
			for (idx_t i = offset; i < next_offset; ++i) {
				uint32_t low_bits = static_cast<uint32_t>((start_row_index + i) & 0xFFFFFFFF);
				const bool is_deleted = bitmap.containsBulk(bulk_context, low_bits);
				result_sel.set_index(selection_idx, i);
				selection_idx += !is_deleted;
			}
		}

		offset = next_offset;
	}
	return selection_idx;
}

unique_ptr<DeleteFilter> IcebergDeletionVectorData::ToFilter() const {
	return make_uniq<IcebergDeletionVector>(shared_from_this());
}

namespace {

struct RoaringIterateContext {
	set<idx_t> *out;
	idx_t high;
};

} // namespace

void IcebergDeletionVectorData::ToSet(set<idx_t> &out) const {
	for (auto &entry : bitmaps) {
		RoaringIterateContext ctx {&out, static_cast<idx_t>(entry.first)};
		auto &bitmap = entry.second;

		bitmap.iterate(
		    [](uint32_t value, void *ptr) -> bool {
			    auto *ctx = static_cast<RoaringIterateContext *>(ptr);
			    idx_t full_value = (ctx->high << 32) | static_cast<idx_t>(value);
			    ctx->out->insert(full_value);
			    return true;
		    },
		    &ctx);
	}
}

vector<data_t> IcebergDeletionVectorData::ToBlob(const unordered_map<int32_t, roaring::Roaring> &bitmaps) {
	//! https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type

	// Calculate total size needed
	idx_t total_size = 0;
	total_size += sizeof(uint32_t); // vector_size field
	total_size += sizeof(uint32_t); // magic bytes
	total_size += sizeof(uint64_t); // amount of bitmaps
	for (const auto &entry : bitmaps) {
		total_size += sizeof(int32_t);                   // key
		total_size += entry.second.getSizeInBytes(true); // portable serialized bitmap
	}
	total_size += sizeof(uint32_t); // CRC checksum

	vector<data_t> blob_output;
	blob_output.resize(total_size);
	data_ptr_t blob_ptr = blob_output.data();

	// Write vector_size (total_size - (CRC checksum + vector_size field))
	uint32_t vector_size = BSwap(static_cast<uint32_t>(total_size - sizeof(uint32_t) - sizeof(uint32_t)));
	Store<uint32_t>(vector_size, blob_ptr);
	blob_ptr += sizeof(uint32_t);

	auto checksummed_data_start = blob_ptr;
	constexpr uint8_t DELETION_VECTOR_MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};
	memcpy(blob_ptr, DELETION_VECTOR_MAGIC, 4);
	blob_ptr += sizeof(uint32_t);

	// Write each bitmap
	Store<uint64_t>(bitmaps.size(), blob_ptr);
	blob_ptr += sizeof(uint64_t);
	for (const auto &entry : bitmaps) {
		// Write key
		Store<int32_t>(entry.first, blob_ptr);
		blob_ptr += sizeof(int32_t);

		// Write bitmap
		size_t bitmap_size = entry.second.write((char *)blob_ptr, true);
		blob_ptr += bitmap_size;
	}

	auto checksummed_data_length = blob_ptr - checksummed_data_start;
	CRC32 crc;
	crc.Update(checksummed_data_start, checksummed_data_length);
	uint32_t checksum = crc.GetValue();

	// Write CRC checksum (placeholder - set to 0)
	Store<uint32_t>(BSwap(checksum), blob_ptr);
	return blob_output;
}

vector<data_t> IcebergDeletionVectorData::ToPuffinFile(const vector<data_t> &blob, const string &referenced_data_file,
                                                       idx_t cardinality) {
	//! Wrap a `deletion-vector-v1` blob in a valid Puffin file container.
	//! https://iceberg.apache.org/puffin-spec/
	//! File layout:   Magic | Blob | Footer
	//! Footer layout: Magic | FooterPayload (JSON) | FooterPayloadSize (4 bytes, little-endian) |
	//!                Flags (4 bytes) | Magic
	constexpr data_t PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31}; //! "PFA1"
	const idx_t blob_offset = sizeof(PUFFIN_MAGIC);

	//! Build the FooterPayload (FileMetadata). Per the spec, for `deletion-vector-v1` the blob's
	//! `snapshot-id` and `sequence-number` must be -1, and it must carry the `referenced-data-file`
	//! and `cardinality` properties. The blob is not compressed (no `compression-codec`).
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	auto doc = doc_p.get();
	auto root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	auto blobs = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, root, "blobs", blobs);
	auto blob_meta = yyjson_mut_obj(doc);
	yyjson_mut_arr_add_val(blobs, blob_meta);
	yyjson_mut_obj_add_val(doc, blob_meta, "type", yyjson_mut_str(doc, "deletion-vector-v1"));
	yyjson_mut_obj_add_val(doc, blob_meta, "fields", yyjson_mut_arr(doc));
	yyjson_mut_obj_add_val(doc, blob_meta, "snapshot-id", yyjson_mut_int(doc, -1));
	yyjson_mut_obj_add_val(doc, blob_meta, "sequence-number", yyjson_mut_int(doc, -1));
	yyjson_mut_obj_add_val(doc, blob_meta, "offset", yyjson_mut_int(doc, static_cast<int64_t>(blob_offset)));
	yyjson_mut_obj_add_val(doc, blob_meta, "length", yyjson_mut_int(doc, static_cast<int64_t>(blob.size())));
	auto props = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, blob_meta, "properties", props);
	yyjson_mut_obj_add_strcpy(doc, props, "referenced-data-file", referenced_data_file.c_str());
	yyjson_mut_obj_add_strcpy(doc, props, "cardinality", std::to_string(cardinality).c_str());
	auto footer_payload = ICUtils::JsonToString(std::move(doc_p));

	const idx_t footer_payload_size = footer_payload.size();
	const idx_t total_size = blob_offset + blob.size() + sizeof(PUFFIN_MAGIC) + footer_payload_size +
	                         sizeof(int32_t) /* FooterPayloadSize */ + sizeof(uint32_t) /* Flags */ +
	                         sizeof(PUFFIN_MAGIC);

	vector<data_t> file_output;
	file_output.resize(total_size);
	data_ptr_t ptr = file_output.data();

	//! Magic
	memcpy(ptr, PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC));
	ptr += sizeof(PUFFIN_MAGIC);
	//! Blob (starts at blob_offset == 4)
	memcpy(ptr, blob.data(), blob.size());
	ptr += blob.size();
	//! Footer: leading Magic
	memcpy(ptr, PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC));
	ptr += sizeof(PUFFIN_MAGIC);
	//! Footer: FooterPayload (uncompressed UTF-8 JSON)
	memcpy(ptr, footer_payload.c_str(), footer_payload_size);
	ptr += footer_payload_size;
	//! Footer: FooterPayloadSize (4-byte signed int, little-endian)
	Store<int32_t>(static_cast<int32_t>(footer_payload_size), ptr);
	ptr += sizeof(int32_t);
	//! Footer: Flags (4 bytes; bit 0 of byte 0 = FooterPayload compressed -> 0 == uncompressed)
	Store<uint32_t>(0, ptr);
	ptr += sizeof(uint32_t);
	//! Footer: trailing Magic
	memcpy(ptr, PUFFIN_MAGIC, sizeof(PUFFIN_MAGIC));

	return file_output;
}

} // namespace duckdb
