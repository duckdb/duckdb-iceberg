#include "deletes/deletion_vector.hpp"
#include "iceberg_multi_file_list.hpp"

#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/common/bswap.hpp"

namespace duckdb {

unique_ptr<IcebergDeletionVector> IcebergDeletionVector::FromBlob(data_ptr_t blob_start, idx_t blob_length) {
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
	memcpy(magic_bytes, blob_start, 4);
	blob_start += 4;
	vector_size -= 4;
	D_ASSERT(blob_start < blob_end);

	if (memcmp(DELETION_VECTOR_MAGIC, magic_bytes, 4)) {
		throw InvalidInputException("Magic bytes mismatch, deletion vector is corrupt!");
	}

	int64_t amount_of_bitmaps = Load<int64_t>(blob_start);
	blob_start += sizeof(int64_t);
	vector_size -= sizeof(int64_t);
	D_ASSERT(blob_start < blob_end);

	auto result_p = make_uniq<IcebergDeletionVector>();
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
	//! The CRC checksum we ignore
	D_ASSERT((blob_end - blob_start) == 4);
	return result_p;
}

void IcebergMultiFileList::ScanPuffinFile(const IcebergManifestEntry &entry) const {
	auto file_path = entry.file_path;
	D_ASSERT(!entry.referenced_data_file.empty());

	auto caching_file_system = CachingFileSystem::Get(context);

	auto caching_file_handle = caching_file_system.OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ);
	data_ptr_t data = nullptr;

	D_ASSERT(!entry.content_offset.IsNull());
	D_ASSERT(!entry.content_size_in_bytes.IsNull());

	auto offset = entry.content_offset.GetValue<int64_t>();
	auto length = entry.content_size_in_bytes.GetValue<int64_t>();

	auto buf_handle = caching_file_handle->Read(data, length, offset);
	auto buffer_data = buf_handle.Ptr();

	positional_delete_data.emplace(entry.referenced_data_file, IcebergDeletionVector::FromBlob(buffer_data, length));
}

idx_t IcebergDeletionVector::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	if (count == 0) {
		return 0;
	}
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t selection_idx = 0;

	idx_t offset = 0;
	while (offset < count) {
		const row_t current_row = start_row_index + offset;
		const int32_t high = static_cast<int32_t>(current_row >> 32);

		const row_t next_high_boundary = ((static_cast<row_t>(high) + 1) << 32);
		//! FIXME: How do we test this? These offsets are **huge**
		const idx_t next_offset = MinValue<idx_t>(start_row_index + count, next_high_boundary) - start_row_index;

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

} // namespace duckdb
