#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

namespace iceberg_avro_codec {

//! Map the `write.manifest.compression-codec` property to the Avro codec name to emit:
//! ""/"gzip"/"deflate" -> "deflate" (Java default), "none"/"null"/"uncompressed" -> "null".
//! Throws on any other value. The codec is validated again by the avro extension at write time.
string ResolveAvroCodec(const string &property_value);

} // namespace iceberg_avro_codec

} // namespace duckdb
