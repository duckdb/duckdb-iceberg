#include "core/expression/iceberg_hash.hpp"
#include "common/iceberg_math.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

//! MurmurHash3 32-bit implementation
//! This is the standard implementation matching Apache Iceberg specification
int32_t IcebergHash::Murmur3Hash32(const uint8_t *data, idx_t len, uint32_t seed) {
	uint32_t h1 = seed;
	const idx_t nblocks = len / 4;

	const uint32_t *blocks = reinterpret_cast<const uint32_t *>(data);
	for (idx_t i = 0; i < nblocks; i++) {
		uint32_t k1 = blocks[i];

		k1 *= C1;
		k1 = RotateLeft(k1, 15);
		k1 *= C2;

		h1 ^= k1;
		h1 = RotateLeft(h1, 13);
		h1 = h1 * 5 + 0xe6546b64;
	}

	const uint8_t *tail = data + nblocks * 4;
	uint32_t k1 = 0;

	switch (len & 3) {
	case 3:
		k1 ^= tail[2] << 16;
		// fallthrough
	case 2:
		k1 ^= tail[1] << 8;
		// fallthrough
	case 1:
		k1 ^= tail[0];
		k1 *= C1;
		k1 = RotateLeft(k1, 15);
		k1 *= C2;
		h1 ^= k1;
	}

	h1 ^= len;
	h1 = FMix32(h1);

	return static_cast<int32_t>(h1);
}

//! Hash int32 value
//! Iceberg spec: BucketUtil.hash(int) calls Guava's MURMUR3.hashLong(value),
//! which sign-extends the int to a long (8 bytes) before hashing.
//! So we must hash the int as a sign-extended int64 (8 bytes), not 4 bytes.
int32_t IcebergHash::HashInt32(int32_t value) {
	return HashInt64(static_cast<int64_t>(value));
}

//! Hash int64 value as 8 bytes (little-endian), matching Java's ByteBuffer.putLong behavior
int32_t IcebergHash::HashInt64(int64_t value) {
	uint8_t bytes[8];
	for (int i = 0; i < 8; i++) {
		bytes[i] = static_cast<uint8_t>(value >> (i * 8));
	}
	return Murmur3Hash32(bytes, 8, SEED);
}

//! Hash string value (Iceberg spec: hash UTF-8 bytes)
int32_t IcebergHash::HashString(const string_t &value) {
	return Murmur3Hash32(reinterpret_cast<const uint8_t *>(value.GetData()), value.GetSize(), SEED);
}

//! Hash date (Iceberg spec: days since epoch as int32, hashed as int64)
int32_t IcebergHash::HashDate(date_t date) {
	return HashInt32(date.days);
}

//! Hash decimal value
//! Iceberg spec: hash the minimum number of bytes required to hold the unscaled value
//! as a two's complement big-endian signed integer.
int32_t IcebergHash::HashDecimal(const Value &value) {
	D_ASSERT(value.type().id() == LogicalTypeId::DECIMAL);

	// Extract the unscaled integer value based on physical storage type
	int64_t unscaled = 0;
	bool is_hugeint = false;
	hugeint_t hugeint_val {};

	switch (value.type().InternalType()) {
	case PhysicalType::INT16:
		unscaled = value.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		unscaled = value.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		unscaled = value.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::INT128:
		is_hugeint = true;
		hugeint_val = value.GetValueUnsafe<hugeint_t>();
		break;
	default:
		return 0;
	}

	// Serialize to minimum bytes, big-endian two's complement
	uint8_t buf[16];
	idx_t byte_len = 0;

	if (is_hugeint) {
		// Write 16 bytes big-endian
		buf[0] = static_cast<uint8_t>(hugeint_val.upper >> 56);
		buf[1] = static_cast<uint8_t>(hugeint_val.upper >> 48);
		buf[2] = static_cast<uint8_t>(hugeint_val.upper >> 40);
		buf[3] = static_cast<uint8_t>(hugeint_val.upper >> 32);
		buf[4] = static_cast<uint8_t>(hugeint_val.upper >> 24);
		buf[5] = static_cast<uint8_t>(hugeint_val.upper >> 16);
		buf[6] = static_cast<uint8_t>(hugeint_val.upper >> 8);
		buf[7] = static_cast<uint8_t>(hugeint_val.upper);
		buf[8] = static_cast<uint8_t>(hugeint_val.lower >> 56);
		buf[9] = static_cast<uint8_t>(hugeint_val.lower >> 48);
		buf[10] = static_cast<uint8_t>(hugeint_val.lower >> 40);
		buf[11] = static_cast<uint8_t>(hugeint_val.lower >> 32);
		buf[12] = static_cast<uint8_t>(hugeint_val.lower >> 24);
		buf[13] = static_cast<uint8_t>(hugeint_val.lower >> 16);
		buf[14] = static_cast<uint8_t>(hugeint_val.lower >> 8);
		buf[15] = static_cast<uint8_t>(hugeint_val.lower);
		byte_len = 16;
	} else {
		// Write 8 bytes big-endian
		for (int i = 7; i >= 0; i--) {
			buf[7 - i] = static_cast<uint8_t>(unscaled >> (i * 8));
		}
		byte_len = 8;
	}

	// Strip leading redundant sign bytes (keep minimum representation)
	// A byte is redundant if it equals the sign of the next byte
	idx_t start = 0;
	while (start < byte_len - 1) {
		uint8_t sign_ext = (buf[start + 1] & 0x80) ? 0xFF : 0x00;
		if (buf[start] == sign_ext) {
			start++;
		} else {
			break;
		}
	}

	return Murmur3Hash32(buf + start, byte_len - start, SEED);
}

//! Hash raw unscaled decimal stored as int64 (covers INT16/INT32/INT64 physical types)
//! Serializes to minimum big-endian two's complement bytes, matching Java BigDecimal.unscaledValue().toByteArray()
int32_t IcebergHash::HashDecimalInt64(int64_t unscaled) {
	uint8_t buf[8];
	for (int i = 7; i >= 0; i--) {
		buf[7 - i] = static_cast<uint8_t>(unscaled >> (i * 8));
	}
	idx_t start = 0;
	while (start < 7) {
		uint8_t sign_ext = (buf[start + 1] & 0x80) ? 0xFF : 0x00;
		if (buf[start] == sign_ext) {
			start++;
		} else {
			break;
		}
	}
	return Murmur3Hash32(buf + start, 8 - start, SEED);
}

//! Hash raw unscaled decimal stored as hugeint (INT128 physical type)
int32_t IcebergHash::HashDecimalHugeInt(hugeint_t unscaled) {
	uint8_t buf[16];
	buf[0] = static_cast<uint8_t>(unscaled.upper >> 56);
	buf[1] = static_cast<uint8_t>(unscaled.upper >> 48);
	buf[2] = static_cast<uint8_t>(unscaled.upper >> 40);
	buf[3] = static_cast<uint8_t>(unscaled.upper >> 32);
	buf[4] = static_cast<uint8_t>(unscaled.upper >> 24);
	buf[5] = static_cast<uint8_t>(unscaled.upper >> 16);
	buf[6] = static_cast<uint8_t>(unscaled.upper >> 8);
	buf[7] = static_cast<uint8_t>(unscaled.upper);
	buf[8] = static_cast<uint8_t>(unscaled.lower >> 56);
	buf[9] = static_cast<uint8_t>(unscaled.lower >> 48);
	buf[10] = static_cast<uint8_t>(unscaled.lower >> 40);
	buf[11] = static_cast<uint8_t>(unscaled.lower >> 32);
	buf[12] = static_cast<uint8_t>(unscaled.lower >> 24);
	buf[13] = static_cast<uint8_t>(unscaled.lower >> 16);
	buf[14] = static_cast<uint8_t>(unscaled.lower >> 8);
	buf[15] = static_cast<uint8_t>(unscaled.lower);
	idx_t start = 0;
	while (start < 15) {
		uint8_t sign_ext = (buf[start + 1] & 0x80) ? 0xFF : 0x00;
		if (buf[start] == sign_ext) {
			start++;
		} else {
			break;
		}
	}
	return Murmur3Hash32(buf + start, 16 - start, SEED);
}

//! Hash time value (Iceberg spec: int64 microseconds from midnight)
int32_t IcebergHash::HashTime(dtime_t t) {
	return HashInt64(t.micros);
}

//! Hash timestamp_ns value (Iceberg spec: int64 nanoseconds from epoch)
int32_t IcebergHash::HashTimestampNs(timestamp_ns_t t) {
	return HashInt64(t.value);
}

//! Hash UUID value (Iceberg spec: 16 big-endian bytes, MSB first)
//! DuckDB stores UUID as hugeint_t: upper (int64, most-significant) + lower (uint64, least-significant)
int32_t IcebergHash::HashUUID(hugeint_t uuid) {
	uint8_t bytes[16];
	for (int i = 0; i < 8; i++) {
		bytes[i] = static_cast<uint8_t>(uuid.upper >> (56 - i * 8));
	}
	for (int i = 0; i < 8; i++) {
		bytes[8 + i] = static_cast<uint8_t>(uuid.lower >> (56 - i * 8));
	}
	return Murmur3Hash32(bytes, 16, SEED);
}

//! Hash a DuckDB Value based on its type
//! Supports Iceberg bucket transform types: integer, long, decimal, date, timestamp, timestamptz, string, binary
int32_t IcebergHash::HashValue(const Value &value) {
	D_ASSERT(!value.IsNull());
	switch (value.type().id()) {
	// integer: sign-extended to int64 before hashing, matching BucketUtil.hash(int)
	case LogicalTypeId::INTEGER:
		return HashInt32(value.GetValue<int32_t>());
	// date: days-since-epoch (int32), sign-extended to int64 before hashing
	case LogicalTypeId::DATE:
		return HashDate(value.GetValue<date_t>());
	// long: hashed directly as int64
	case LogicalTypeId::BIGINT:
		return HashInt64(value.GetValue<int64_t>());
	// string: hashed as UTF-8 bytes
	case LogicalTypeId::VARCHAR: {
		auto str_val = value.ToString();
		return HashString(string_t(str_val));
	}
	// binary: hashed as raw bytes (same algorithm as string)
	case LogicalTypeId::BLOB: {
		auto blob_val = value.GetValueUnsafe<string_t>();
		return Murmur3Hash32(reinterpret_cast<const uint8_t *>(blob_val.GetData()), blob_val.GetSize(), SEED);
	}
	// decimal: hash minimum big-endian two's complement bytes of unscaled value
	case LogicalTypeId::DECIMAL:
		return HashDecimal(value);
	// timestamp / timestamptz: microseconds since epoch, hashed as int64
	case LogicalTypeId::TIMESTAMP:
		return HashInt64(value.GetValue<timestamp_t>().value);
	case LogicalTypeId::TIMESTAMP_TZ:
		return HashInt64(value.GetValue<timestamp_tz_t>().value);
	// time: microseconds from midnight, hashed as int64
	case LogicalTypeId::TIME:
		return HashTime(value.GetValue<dtime_t>());
	// timestamp_ns: Iceberg buckets nanos as micros first (BucketTimestampNano in Java)
	case LogicalTypeId::TIMESTAMP_NS:
		return HashInt64(IcebergNanosToMicrosFloor(value.GetValue<timestamp_ns_t>().value));
	// uuid: 16 big-endian bytes
	case LogicalTypeId::UUID:
		return HashUUID(value.GetValueUnsafe<hugeint_t>());
	default:
		return 0;
	}
}

//! Canonical bucket computation for predicate pruning.
//! Returns Value::INTEGER(bucket_id) for supported types, or a null INTEGER Value for null/unsupported input
//! (null signals the caller to be conservative and not filter the file).
Value IcebergHash::BucketValue(const Value &v, int32_t num_buckets) {
	if (v.IsNull()) {
		return Value(LogicalType::INTEGER);
	}
	switch (v.type().id()) {
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::UUID:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		break;
	default:
		// Unsupported type: return null so predicate pushdown is conservative
		return Value(LogicalType::INTEGER);
	}
	int32_t h = HashValue(v);
	return Value::INTEGER((h & 0x7FFFFFFF) % num_buckets);
}

//! Canonical truncate computation shared by TruncateTransform::ApplyTransform and the scalar function.
//! Input must not be null; throws for unsupported types.
Value IcebergHash::TruncateValue(const Value &v, idx_t width) {
	auto W = static_cast<int64_t>(width);
	switch (v.type().id()) {
	case LogicalTypeId::INTEGER: {
		auto val = static_cast<int64_t>(v.GetValue<int32_t>());
		return Value::INTEGER(static_cast<int32_t>(val - (((val % W) + W) % W)));
	}
	case LogicalTypeId::BIGINT: {
		auto val = v.GetValue<int64_t>();
		return Value::BIGINT(val - (((val % W) + W) % W));
	}
	case LogicalTypeId::DECIMAL: {
		// Truncate the unscaled integer value, preserving type (scale/precision)
		auto scaled = v.Copy();
		scaled.Reinterpret(LogicalType::BIGINT);
		auto val = scaled.GetValue<int64_t>();
		auto result = val - (((val % W) + W) % W);
		return Value::DECIMAL(result, DecimalType::GetWidth(v.type()), DecimalType::GetScale(v.type()));
	}
	case LogicalTypeId::BLOB: {
		auto bytes = StringValue::Get(v);
		auto truncated = bytes.size() < static_cast<size_t>(width) ? bytes.size() : static_cast<size_t>(width);
		return Value::BLOB(reinterpret_cast<const uint8_t *>(bytes.data()), truncated);
	}
	case LogicalTypeId::VARCHAR: {
		auto s = v.GetValue<string>();
		size_t num_chars = 0;
		for (auto cluster : Utf8Proc::GraphemeClusters(s.data(), s.size())) {
			if (++num_chars >= static_cast<size_t>(width)) {
				return Value(s.substr(0, cluster.end));
			}
		}
		return v;
	}
	default:
		throw NotImplementedException("iceberg_truncate: unsupported type %s", v.type().ToString());
	}
}

} // namespace duckdb
