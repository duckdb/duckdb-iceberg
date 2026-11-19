#include "core/deletes/iceberg_deletion_vector.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

namespace {

class CRC32 {
public:
	CRC32() : crc(0xFFFFFFFF) {
	}

public:
	void Update(const data_t *data, idx_t length) {
		auto &crc_table = GetTable();
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
	//! Deletion vectors are read and written by several threads at once, so the table is built by a thread-safe
	//! static initializer
	static const array<uint32_t, 256> &GetTable() {
		static const array<uint32_t, 256> crc_table = [] {
			array<uint32_t, 256> table;
			for (uint32_t i = 0; i < 256; i++) {
				uint32_t c = i;
				for (int j = 0; j < 8; j++) {
					if (c & 1) {
						c = 0xEDB88320 ^ (c >> 1);
					} else {
						c = c >> 1;
					}
				}
				table[i] = c;
			}
			return table;
		}();
		return crc_table;
	}

private:
	uint32_t crc;
};

} // namespace

shared_ptr<IcebergDeletionVectorData> IcebergDeletionVectorData::FromBlob(const IcebergFileIdentity &file,
                                                                          data_ptr_t blob_start, idx_t blob_length) {
	//! https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type
	//! The blob is a 4-byte length field, the magic bytes, the bitmaps and a 4-byte checksum. It comes from a file,
	//! so every read is checked against the blob length before it happens.

	//! The length field, the magic bytes, the bitmap count and the checksum
	constexpr idx_t MIN_BLOB_LENGTH = sizeof(uint32_t) + 4 + sizeof(int64_t) + sizeof(uint32_t);
	if (blob_length < MIN_BLOB_LENGTH) {
		throw InvalidInputException("Blob is too small (length of %d bytes) to be a deletion-vector-v1", blob_length);
	}

	//! The length field covers the magic bytes and the bitmaps, so it must equal the blob length minus the length
	//! field and the checksum. Iceberg Java refuses a blob otherwise.
	auto vector_size = BSwap(Load<uint32_t>(blob_start));
	blob_start += sizeof(uint32_t);
	idx_t expected_vector_size = blob_length - 2 * sizeof(uint32_t);
	if (vector_size != expected_vector_size) {
		throw InvalidInputException("Invalid bitmap data length (%d) in deletion vector of %d bytes, expected %d, the "
		                            "DeletionVector is corrupted",
		                            vector_size, blob_length, expected_vector_size);
	}
	//! Where the bitmaps end and the checksum starts
	auto vector_end = blob_start + vector_size;

	constexpr char DELETION_VECTOR_MAGIC[] = {'\xD1', '\xD3', '\x39', '\x64'};
	char magic_bytes[4];

	auto checksummed_data_start = blob_start;
	memcpy(magic_bytes, blob_start, 4);
	blob_start += 4;

	auto memcmp_res = memcmp(DELETION_VECTOR_MAGIC, magic_bytes, 4);
	if (memcmp_res != 0) {
		throw InvalidInputException("Magic bytes mismatch, deletion vector is corrupt!");
	}

	int64_t amount_of_bitmaps = Load<int64_t>(blob_start);
	blob_start += sizeof(int64_t);

	//! Every bitmap takes at least its key, so the declared count is bounded by the bytes before the checksum
	auto remaining_bytes = NumericCast<idx_t>(vector_end - blob_start);
	if (amount_of_bitmaps < 0 || NumericCast<idx_t>(amount_of_bitmaps) > remaining_bytes / sizeof(int32_t)) {
		throw InvalidInputException("Invalid bitmap count (%d) in deletion vector of %d bytes, the DeletionVector is "
		                            "corrupted",
		                            amount_of_bitmaps, blob_length);
	}

	auto result_p = make_shared_ptr<IcebergDeletionVectorData>(file);
	auto &result = *result_p;
	result.bitmaps.reserve(amount_of_bitmaps);
	for (int64_t i = 0; i < amount_of_bitmaps; i++) {
		if (NumericCast<idx_t>(vector_end - blob_start) < sizeof(int32_t)) {
			throw InvalidInputException("Deletion vector is truncated, the DeletionVector is corrupted");
		}
		auto key = Load<int32_t>(blob_start);
		blob_start += sizeof(int32_t);

		//! roaring reads at most the bytes before the checksum, and returns 0 if no valid bitmap fits in them
		auto available = NumericCast<idx_t>(vector_end - blob_start);
		size_t bitmap_size =
		    roaring::api::roaring_bitmap_portable_deserialize_size((const char *)blob_start, available);
		if (bitmap_size == 0) {
			throw InvalidInputException("Invalid bitmap in deletion vector, the DeletionVector is corrupted");
		}
		//! readSafe throws a plain std::runtime_error when it fails, so call the C function and check for NULL
		auto bitmap_p = roaring::api::roaring_bitmap_portable_deserialize_safe((const char *)blob_start, bitmap_size);
		if (!bitmap_p) {
			throw InvalidInputException("Invalid bitmap in deletion vector, the DeletionVector is corrupted");
		}
		roaring::Roaring bitmap(bitmap_p);
		blob_start += bitmap_size;
		result.bitmaps.emplace(key, std::move(bitmap));
	}
	//! The bitmaps must fill the blob up to the checksum
	if (blob_start != vector_end) {
		throw InvalidInputException("Deletion vector has %d bytes after its bitmaps, the DeletionVector is corrupted",
		                            NumericCast<idx_t>(vector_end - blob_start));
	}

	//! Compute and compare the checksum, the last 4 bytes of the blob
	auto checksummed_data_length = blob_start - checksummed_data_start;
	auto stored_checksum = BSwap(Load<uint32_t>(blob_start));

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
		if (!current_high || *current_high != high) {
			auto it = bitmaps.find(high);
			if (it == bitmaps.end()) {
				current_bitmap = nullptr;
			} else {
				current_bitmap = it->second;
				bulk_context = roaring::BulkContext();
			}
			current_high = high;
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

} // namespace duckdb
