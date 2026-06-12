#include "core/metadata/manifest/iceberg_avro_codec.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "miniz_wrapper.hpp"

namespace duckdb {

namespace iceberg_avro_codec {

//===--------------------------------------------------------------------===//
// Avro Object Container File (OCF) primitives
//===--------------------------------------------------------------------===//
// OCF layout (Avro spec):
//   magic        : 4 bytes  = 'O','b','j', 0x01
//   metadata     : Avro map<string,bytes>  (block-count [, byte-count]? entries..., 0 terminator)
//   sync marker  : 16 bytes
//   data blocks  : repeated { long object_count, long byte_count, <byte_count bytes>, 16-byte sync }
// Integers are zig-zag varint ("long") encoded. The block payload is encoded with the codec named
// by the "avro.codec" metadata key ("null" = raw, "deflate" = raw DEFLATE, no zlib/gzip wrapper).

namespace {

static const uint8_t AVRO_MAGIC[4] = {'O', 'b', 'j', 0x01};
constexpr idx_t SYNC_SIZE = 16;

//! Sequential reader over an in-memory Avro file.
struct AvroReader {
	AvroReader(const_data_ptr_t data, idx_t size) : data(data), size(size) {
	}

	const_data_ptr_t data;
	idx_t size;
	idx_t pos = 0;

	void Require(idx_t n) {
		//! n comes from a varint length in the file; guard against overflow (a corrupt/truncated file
		//! could yield a huge length that wraps pos + n) as well as a plain out-of-bounds read.
		if (n > size || pos > size - n) {
			throw InvalidInputException("Corrupt Avro manifest: unexpected end of file while recompressing");
		}
	}

	uint8_t ReadByte() {
		Require(1);
		return data[pos++];
	}

	//! Avro long: zig-zag + variable-length (LEB128-style, 7 bits per byte, MSB = continuation).
	int64_t ReadLong() {
		uint64_t value = 0;
		int shift = 0;
		while (true) {
			auto b = ReadByte();
			value |= (uint64_t)(b & 0x7F) << shift;
			if (!(b & 0x80)) {
				break;
			}
			shift += 7;
			if (shift > 63) {
				throw InvalidInputException("Corrupt Avro manifest: overlong varint while recompressing");
			}
		}
		//! zig-zag decode
		return (int64_t)((value >> 1) ^ (~(value & 1) + 1));
	}

	const_data_ptr_t ReadRaw(idx_t n) {
		Require(n);
		auto ptr = data + pos;
		pos += n;
		return ptr;
	}
};

//! Sequential writer into a growable byte buffer.
struct AvroWriter {
	vector<data_t> buffer;

	void WriteByte(uint8_t b) {
		buffer.push_back(b);
	}

	void WriteLong(int64_t value) {
		//! zig-zag encode
		uint64_t zz = (uint64_t)((value << 1) ^ (value >> 63));
		do {
			uint8_t b = zz & 0x7F;
			zz >>= 7;
			if (zz) {
				b |= 0x80;
			}
			buffer.push_back(b);
		} while (zz);
	}

	void WriteRaw(const_data_ptr_t ptr, idx_t n) {
		buffer.insert(buffer.end(), ptr, ptr + n);
	}

	void WriteString(const string &str) {
		WriteLong((int64_t)str.size());
		WriteRaw(const_data_ptr_cast(str.data()), str.size());
	}

	//! bytes = long length + raw bytes
	void WriteBytes(const_data_ptr_t ptr, idx_t n) {
		WriteLong((int64_t)n);
		WriteRaw(ptr, n);
	}
};

//! Raw DEFLATE (negative window bits => no zlib header/adler footer), matching Avro's "deflate"
//! codec. Uses DuckDB's vendored miniz directly because MiniZStream::Compress adds a gzip wrapper.
static vector<data_t> RawDeflate(const_data_ptr_t input, idx_t input_size) {
	duckdb_miniz::mz_stream stream;
	memset(&stream, 0, sizeof(stream));
	//! Negative window bits => raw DEFLATE (no zlib header/adler footer), which is Avro's "deflate"
	//! codec. MZ_DEFLATED / MZ_DEFAULT_WINDOW_BITS are macros (not namespaced); the levels/flush
	//! values are enums in duckdb_miniz.
	auto ret = duckdb_miniz::mz_deflateInit2(&stream, duckdb_miniz::MZ_DEFAULT_LEVEL, MZ_DEFLATED,
	                                         -MZ_DEFAULT_WINDOW_BITS, 1, 0);
	if (ret != duckdb_miniz::MZ_OK) {
		throw InvalidInputException("Failed to initialize miniz for Avro deflate codec");
	}

	vector<data_t> out;
	out.resize(duckdb_miniz::mz_deflateBound(&stream, input_size));

	stream.next_in = reinterpret_cast<const unsigned char *>(input);
	stream.avail_in = (unsigned int)input_size;
	stream.next_out = reinterpret_cast<unsigned char *>(out.data());
	stream.avail_out = (unsigned int)out.size();

	ret = duckdb_miniz::mz_deflate(&stream, duckdb_miniz::MZ_FINISH);
	if (ret != duckdb_miniz::MZ_STREAM_END) {
		duckdb_miniz::mz_deflateEnd(&stream);
		throw InvalidInputException("Failed to deflate Avro manifest block");
	}
	auto compressed_size = stream.total_out;
	duckdb_miniz::mz_deflateEnd(&stream);

	out.resize(compressed_size);
	return out;
}

} // namespace

string ResolveAvroCodec(const string &property_value, bool catalog_allows_deletes) {
	//! Compression needs a deletable temp file (the COPY function cannot emit a codec). Catalogs that
	//! forbid client deletes (e.g. S3 Tables) cannot clean it up, so force uncompressed there; they
	//! manage their own storage optimization anyway.
	if (!catalog_allows_deletes) {
		return "null";
	}
	//! Default matches Java's TableProperties.AVRO_COMPRESSION default ("gzip" -> Avro "deflate").
	if (property_value.empty()) {
		return "deflate";
	}
	if (StringUtil::CIEquals(property_value, "gzip") || StringUtil::CIEquals(property_value, "deflate")) {
		return "deflate";
	}
	if (StringUtil::CIEquals(property_value, "none") || StringUtil::CIEquals(property_value, "null") ||
	    StringUtil::CIEquals(property_value, "uncompressed")) {
		return "null";
	}
	//! Avro also defines snappy/zstd/bzip2, but DuckDB's vendored miniz only provides DEFLATE. Fail
	//! loudly rather than silently writing an uncompressed file the user did not ask for.
	throw NotImplementedException(
	    "Unsupported value '%s' for 'write.manifest.compression-codec'; this extension supports 'gzip'/'deflate' and "
	    "'none'/'uncompressed'",
	    property_value);
}

bool RequiresRecompression(const string &avro_codec) {
	return !avro_codec.empty() && !StringUtil::CIEquals(avro_codec, "null");
}

idx_t RecompressManifestFile(ClientContext &context, const string &source_path, const string &dest_path,
                             const string &avro_codec) {
	if (!StringUtil::CIEquals(avro_codec, "deflate")) {
		throw NotImplementedException("Avro codec '%s' is not supported for manifest compression", avro_codec);
	}

	auto &fs = FileSystem::GetFileSystem(context);

	//! Read the whole (small) uncompressed manifest into memory. Use the location-based Read, which
	//! loops until every requested byte is read (or throws); the plain Read(buffer, n) can short-read
	//! on object stores, which would leave the tail uninitialized and corrupt the parse.
	vector<data_t> input;
	{
		auto handle = fs.OpenFile(source_path, FileFlags::FILE_FLAGS_READ);
		auto file_size = handle->GetFileSize();
		input.resize(file_size);
		if (file_size > 0) {
			handle->Read(input.data(), file_size, 0);
		}
	}

	AvroReader reader(input.data(), input.size());

	//! Magic.
	auto magic = reader.ReadRaw(sizeof(AVRO_MAGIC));
	if (memcmp(magic, AVRO_MAGIC, sizeof(AVRO_MAGIC)) != 0) {
		throw InvalidInputException("Corrupt Avro manifest: bad magic while recompressing '%s'", source_path);
	}

	//! File metadata: a map<string,bytes>. Read all entries, overriding avro.codec to deflate.
	//! A map is a sequence of blocks; each block is a (possibly negative) long count, optionally
	//! followed by a byte-size long when count is negative, then that many key/value pairs, ending
	//! with a 0 count.
	vector<std::pair<string, vector<data_t>>> metadata;
	while (true) {
		auto block_count = reader.ReadLong();
		if (block_count == 0) {
			break;
		}
		if (block_count < 0) {
			//! Negative count => the next long is the block byte size (which we can ignore).
			block_count = -block_count;
			reader.ReadLong();
		}
		for (int64_t i = 0; i < block_count; i++) {
			auto key_len = reader.ReadLong();
			auto key_ptr = reader.ReadRaw((idx_t)key_len);
			string key(const_char_ptr_cast(key_ptr), (idx_t)key_len);

			auto val_len = reader.ReadLong();
			auto val_ptr = reader.ReadRaw((idx_t)val_len);
			vector<data_t> val(val_ptr, val_ptr + (idx_t)val_len);

			metadata.emplace_back(std::move(key), std::move(val));
		}
	}

	//! Override (or add) avro.codec = deflate.
	bool codec_set = false;
	const string deflate = "deflate";
	for (auto &entry : metadata) {
		if (entry.first == "avro.codec") {
			entry.second.assign(deflate.begin(), deflate.end());
			codec_set = true;
		}
	}
	if (!codec_set) {
		vector<data_t> val(deflate.begin(), deflate.end());
		metadata.emplace_back("avro.codec", std::move(val));
	}

	//! Sync marker (16 bytes), reused verbatim for every block we re-emit.
	auto sync_ptr = reader.ReadRaw(SYNC_SIZE);
	vector<data_t> sync(sync_ptr, sync_ptr + SYNC_SIZE);

	//! Build the new file.
	AvroWriter writer;
	writer.WriteRaw(AVRO_MAGIC, sizeof(AVRO_MAGIC));

	//! Metadata map: single block with all entries, then a 0 terminator.
	writer.WriteLong((int64_t)metadata.size());
	for (auto &entry : metadata) {
		writer.WriteString(entry.first);
		writer.WriteBytes(entry.second.data(), entry.second.size());
	}
	writer.WriteLong(0);
	writer.WriteRaw(sync.data(), sync.size());

	//! Data blocks: object_count, byte_count, payload, sync. Recompress each payload.
	while (reader.pos < reader.size) {
		auto object_count = reader.ReadLong();
		auto byte_count = reader.ReadLong();
		auto payload = reader.ReadRaw((idx_t)byte_count);
		//! The original sync marker follows each block; consume and verify it matches.
		auto block_sync = reader.ReadRaw(SYNC_SIZE);
		if (memcmp(block_sync, sync.data(), SYNC_SIZE) != 0) {
			throw InvalidInputException("Corrupt Avro manifest: sync marker mismatch while recompressing '%s'",
			                            source_path);
		}

		auto compressed = RawDeflate(payload, (idx_t)byte_count);

		writer.WriteLong(object_count);
		writer.WriteLong((int64_t)compressed.size());
		writer.WriteRaw(compressed.data(), compressed.size());
		writer.WriteRaw(sync.data(), sync.size());
	}

	//! Write the compressed container as a brand-new file. FILE_CREATE_NEW (never reopening an
	//! existing object for overwrite/truncate) is what object stores support; dest_path is a path no
	//! one has written yet (the caller routes the COPY output to a separate temp source_path).
	{
		auto handle = fs.OpenFile(dest_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		if (!writer.buffer.empty()) {
			handle->Write(writer.buffer.data(), writer.buffer.size());
		}
		handle->Close();
	}
	//! We know exactly how many bytes we wrote -> return it so the caller avoids a size-probe (HEAD).
	return writer.buffer.size();
}

} // namespace iceberg_avro_codec

} // namespace duckdb
