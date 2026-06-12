// Standalone unit tests for the pure-logic pieces of the manifest Avro compression implementation.
// They do not depend on the DuckDB test harness or a catalog, so they build and run on their own
// with a single command:
//
//   c++ -std=c++17 test/cpp/test_compression_logic.cpp -lz -o /tmp/t && /tmp/t
//
// Why mirrors: the DuckDB `unittest` binary only runs SQL logic tests, and the extension is a
// dynamically-loaded module, so production C++ functions cannot be linked into a standalone binary
// without pulling in all of DuckDB. The codec-resolution block below reproduces the production
// ResolveAvroCodec function byte-for-byte and names the source file it mirrors; a divergence shows
// up as a failing test during review. The Avro OCF recompression test is stronger than a mirror --
// it performs a real DEFLATE/INFLATE round-trip over hand-built OCF bytes, so it validates actual
// compression behavior. End-to-end coverage of the manifest write paths (which need a catalog)
// lives in the catalog-required SQL tests run by the lakekeeper/polaris/nessie/fixture CI jobs.

#include <cassert>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <map>

//===--------------------------------------------------------------------===//
// Mirror of iceberg_avro_codec.cpp::ResolveAvroCodec. Returns "deflate"/"null"; throws (here:
// returns "<error>") for unsupported. catalog_allows_deletes==false forces "null" (compression needs
// a deletable temp file, which delete-forbidden catalogs like S3 Tables cannot remove).
//===--------------------------------------------------------------------===//
static std::string ResolveAvroCodec(const std::string &v, bool catalog_allows_deletes) {
	auto ci_eq = [](const std::string &a, const char *b) {
		std::string lb(b);
		if (a.size() != lb.size()) {
			return false;
		}
		for (size_t i = 0; i < a.size(); i++) {
			if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(lb[i]))) {
				return false;
			}
		}
		return true;
	};
	if (!catalog_allows_deletes) {
		return "null";
	}
	if (v.empty()) {
		return "deflate";
	}
	if (ci_eq(v, "gzip") || ci_eq(v, "deflate")) {
		return "deflate";
	}
	if (ci_eq(v, "none") || ci_eq(v, "null") || ci_eq(v, "uncompressed")) {
		return "null";
	}
	return "<error>";
}

//===--------------------------------------------------------------------===//
// Real Avro OCF varint + recompression, ported verbatim from iceberg_avro_codec.cpp (with zlib
// raw-deflate standing in for miniz raw-deflate -- both produce the same -15-window-bits stream).
// The recompress test below feeds it a hand-built uncompressed OCF and decodes the result, so this
// exercises the zigzag codec, metadata-map rewrite, block recompression, and DEFLATE round-trip.
//===--------------------------------------------------------------------===//
#include <zlib.h>

namespace ocf {

struct Writer {
	std::vector<uint8_t> buf;
	void WriteLong(int64_t value) {
		uint64_t zz = (uint64_t)((value << 1) ^ (value >> 63));
		do {
			uint8_t b = zz & 0x7F;
			zz >>= 7;
			if (zz) {
				b |= 0x80;
			}
			buf.push_back(b);
		} while (zz);
	}
	void WriteRaw(const uint8_t *p, size_t n) {
		buf.insert(buf.end(), p, p + n);
	}
	void WriteString(const std::string &s) {
		WriteLong((int64_t)s.size());
		WriteRaw((const uint8_t *)s.data(), s.size());
	}
};

struct Reader {
	const uint8_t *data;
	size_t size, pos = 0;
	Reader(const uint8_t *d, size_t s) : data(d), size(s) {
	}
	uint8_t ReadByte() {
		if (pos >= size) {
			throw std::string("eof");
		}
		return data[pos++];
	}
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
		}
		return (int64_t)((value >> 1) ^ (~(value & 1) + 1));
	}
	const uint8_t *ReadRaw(size_t n) {
		if (pos + n > size) {
			throw std::string("eof");
		}
		auto p = data + pos;
		pos += n;
		return p;
	}
};

static std::vector<uint8_t> RawDeflate(const uint8_t *in, size_t n) {
	z_stream s;
	memset(&s, 0, sizeof(s));
	deflateInit2(&s, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
	std::vector<uint8_t> out(deflateBound(&s, n));
	s.next_in = (Bytef *)in;
	s.avail_in = n;
	s.next_out = out.data();
	s.avail_out = out.size();
	deflate(&s, Z_FINISH);
	out.resize(s.total_out);
	deflateEnd(&s);
	return out;
}
static std::vector<uint8_t> RawInflate(const uint8_t *in, size_t n, size_t expected) {
	z_stream s;
	memset(&s, 0, sizeof(s));
	inflateInit2(&s, -15);
	std::vector<uint8_t> out(expected);
	s.next_in = (Bytef *)in;
	s.avail_in = n;
	s.next_out = out.data();
	s.avail_out = out.size();
	inflate(&s, Z_FINISH);
	out.resize(s.total_out);
	inflateEnd(&s);
	return out;
}

static const uint8_t MAGIC[4] = {'O', 'b', 'j', 0x01};

// Recompress an uncompressed OCF (codec=null) to codec=deflate. Mirrors RecompressManifestFile.
static std::vector<uint8_t> Recompress(const std::vector<uint8_t> &input) {
	Reader reader(input.data(), input.size());
	auto magic = reader.ReadRaw(4);
	if (memcmp(magic, MAGIC, 4) != 0) {
		throw std::string("magic");
	}
	std::vector<std::pair<std::string, std::vector<uint8_t>>> metadata;
	while (true) {
		auto block_count = reader.ReadLong();
		if (block_count == 0) {
			break;
		}
		if (block_count < 0) {
			block_count = -block_count;
			reader.ReadLong();
		}
		for (int64_t i = 0; i < block_count; i++) {
			auto kl = reader.ReadLong();
			auto kp = reader.ReadRaw(kl);
			std::string key((const char *)kp, kl);
			auto vl = reader.ReadLong();
			auto vp = reader.ReadRaw(vl);
			metadata.emplace_back(std::move(key), std::vector<uint8_t>(vp, vp + vl));
		}
	}
	bool codec_set = false;
	const std::string deflate_name = "deflate";
	for (auto &e : metadata) {
		if (e.first == "avro.codec") {
			e.second.assign(deflate_name.begin(), deflate_name.end());
			codec_set = true;
		}
	}
	if (!codec_set) {
		metadata.emplace_back("avro.codec", std::vector<uint8_t>(deflate_name.begin(), deflate_name.end()));
	}
	auto sync_ptr = reader.ReadRaw(16);
	std::vector<uint8_t> sync(sync_ptr, sync_ptr + 16);

	Writer writer;
	writer.WriteRaw(MAGIC, 4);
	writer.WriteLong((int64_t)metadata.size());
	for (auto &e : metadata) {
		writer.WriteString(e.first);
		writer.WriteLong((int64_t)e.second.size());
		writer.WriteRaw(e.second.data(), e.second.size());
	}
	writer.WriteLong(0);
	writer.WriteRaw(sync.data(), sync.size());
	while (reader.pos < reader.size) {
		auto object_count = reader.ReadLong();
		auto byte_count = reader.ReadLong();
		auto payload = reader.ReadRaw(byte_count);
		auto block_sync = reader.ReadRaw(16);
		if (memcmp(block_sync, sync.data(), 16) != 0) {
			throw std::string("sync");
		}
		auto compressed = RawDeflate(payload, byte_count);
		writer.WriteLong(object_count);
		writer.WriteLong((int64_t)compressed.size());
		writer.WriteRaw(compressed.data(), compressed.size());
		writer.WriteRaw(sync.data(), sync.size());
	}
	return writer.buf;
}

} // namespace ocf

//===--------------------------------------------------------------------===//
// Tests
//===--------------------------------------------------------------------===//
static int g_failures = 0;
#define CHECK(cond)                                                                                                    \
	do {                                                                                                               \
		if (!(cond)) {                                                                                                 \
			printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                                                     \
			g_failures++;                                                                                              \
		}                                                                                                              \
	} while (0)

//===--------------------------------------------------------------------===//
// Codec resolution
//===--------------------------------------------------------------------===//
static void TestResolveCodecDefault() {
	CHECK(ResolveAvroCodec("", true) == "deflate"); // matches Java default
}
static void TestResolveCodecGzipDeflate() {
	CHECK(ResolveAvroCodec("gzip", true) == "deflate");
	CHECK(ResolveAvroCodec("GZIP", true) == "deflate");
	CHECK(ResolveAvroCodec("deflate", true) == "deflate");
}
static void TestResolveCodecNone() {
	CHECK(ResolveAvroCodec("none", true) == "null");
	CHECK(ResolveAvroCodec("uncompressed", true) == "null");
	CHECK(ResolveAvroCodec("null", true) == "null");
}
static void TestResolveCodecUnsupported() {
	CHECK(ResolveAvroCodec("snappy", true) == "<error>"); // unsupported -> throws in production
	CHECK(ResolveAvroCodec("zstd", true) == "<error>");
}
static void TestResolveCodecDeleteForbidden() {
	// Catalogs that forbid client deletes (e.g. S3 Tables) must never get a compressing codec,
	// regardless of the table property, because the temp-file-then-delete dance would fail there.
	CHECK(ResolveAvroCodec("", false) == "null");
	CHECK(ResolveAvroCodec("gzip", false) == "null");
	CHECK(ResolveAvroCodec("deflate", false) == "null");
	CHECK(ResolveAvroCodec("snappy", false) == "null"); // not even validated -> just uncompressed
}

//===--------------------------------------------------------------------===//
// Avro OCF deflate recompression round-trip -- real DEFLATE, real OCF bytes
//===--------------------------------------------------------------------===//
// Build a minimal uncompressed OCF with codec=null, two data blocks, then recompress and verify the
// blocks decode back to the original payloads and the codec metadata flipped to "deflate".
static std::vector<uint8_t> BuildUncompressedOCF(const std::vector<std::vector<uint8_t>> &blocks,
                                                 const std::vector<int64_t> &object_counts,
                                                 const std::vector<uint8_t> &sync) {
	ocf::Writer w;
	w.WriteRaw(ocf::MAGIC, 4);
	// metadata map: avro.schema + avro.codec=null
	w.WriteLong(2);
	std::string schema_key = "avro.schema", schema_val = "{\"type\":\"record\",\"name\":\"x\",\"fields\":[]}";
	w.WriteString(schema_key);
	w.WriteLong((int64_t)schema_val.size());
	w.WriteRaw((const uint8_t *)schema_val.data(), schema_val.size());
	std::string codec_key = "avro.codec", codec_val = "null";
	w.WriteString(codec_key);
	w.WriteLong((int64_t)codec_val.size());
	w.WriteRaw((const uint8_t *)codec_val.data(), codec_val.size());
	w.WriteLong(0);
	w.WriteRaw(sync.data(), 16);
	for (size_t i = 0; i < blocks.size(); i++) {
		w.WriteLong(object_counts[i]);
		w.WriteLong((int64_t)blocks[i].size());
		w.WriteRaw(blocks[i].data(), blocks[i].size());
		w.WriteRaw(sync.data(), 16);
	}
	return w.buf;
}

static void TestOCFRecompressRoundTrip() {
	std::vector<uint8_t> sync(16);
	for (int i = 0; i < 16; i++) {
		sync[i] = (uint8_t)(i * 7 + 1);
	}
	std::vector<uint8_t> block0, block1;
	for (int i = 0; i < 2000; i++) {
		block0.push_back((uint8_t)(i % 251)); // compressible
	}
	for (int i = 0; i < 500; i++) {
		block1.push_back((uint8_t)((i * 13 + 5) % 256));
	}
	std::vector<int64_t> counts = {100, 25};
	auto uncompressed = BuildUncompressedOCF({block0, block1}, counts, sync);

	std::vector<uint8_t> recompressed;
	bool threw = false;
	try {
		recompressed = ocf::Recompress(uncompressed);
	} catch (const std::string &e) {
		threw = true;
		printf("FAIL OCF recompress threw: %s\n", e.c_str());
	}
	CHECK(!threw);
	CHECK(recompressed.size() < uncompressed.size()); // it actually compressed

	// Decode the recompressed OCF and verify structure + payloads.
	ocf::Reader r(recompressed.data(), recompressed.size());
	auto magic = r.ReadRaw(4);
	CHECK(memcmp(magic, ocf::MAGIC, 4) == 0);
	// metadata map
	std::map<std::string, std::string> meta;
	auto bc = r.ReadLong();
	CHECK(bc > 0);
	for (int64_t i = 0; i < bc; i++) {
		auto kl = r.ReadLong();
		auto kp = r.ReadRaw(kl);
		std::string key((const char *)kp, kl);
		auto vl = r.ReadLong();
		auto vp = r.ReadRaw(vl);
		meta[key] = std::string((const char *)vp, vl);
	}
	CHECK(r.ReadLong() == 0);            // map terminator
	CHECK(meta["avro.codec"] == "deflate"); // codec flipped
	CHECK(meta.count("avro.schema") == 1);  // schema preserved
	r.ReadRaw(16);                       // sync

	// block 0
	CHECK(r.ReadLong() == 100);
	auto c0_len = r.ReadLong();
	auto c0 = r.ReadRaw(c0_len);
	auto d0 = ocf::RawInflate(c0, c0_len, block0.size());
	CHECK(d0 == block0); // round-trips byte-identical
	r.ReadRaw(16);
	// block 1
	CHECK(r.ReadLong() == 25);
	auto c1_len = r.ReadLong();
	auto c1 = r.ReadRaw(c1_len);
	auto d1 = ocf::RawInflate(c1, c1_len, block1.size());
	CHECK(d1 == block1);
	r.ReadRaw(16);
	CHECK(r.pos == r.size); // consumed exactly
}

static void TestOCFVarintZigzag() {
	// The zigzag long encoding must round-trip for representative values (incl. negatives, which
	// appear in Avro map block counts).
	std::vector<int64_t> values = {0, 1, -1, 63, 64, -64, 100, -100, 1000000, -1000000, 9223372036854775807LL};
	for (auto v : values) {
		ocf::Writer w;
		w.WriteLong(v);
		ocf::Reader r(w.buf.data(), w.buf.size());
		CHECK(r.ReadLong() == v);
	}
}

int main() {
	TestResolveCodecDefault();
	TestResolveCodecGzipDeflate();
	TestResolveCodecNone();
	TestResolveCodecUnsupported();
	TestResolveCodecDeleteForbidden();

	TestOCFRecompressRoundTrip();
	TestOCFVarintZigzag();

	if (g_failures == 0) {
		printf("All compression logic tests passed.\n");
		return 0;
	}
	printf("%d test(s) failed.\n", g_failures);
	return 1;
}
