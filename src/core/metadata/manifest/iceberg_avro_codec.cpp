#include "core/metadata/manifest/iceberg_avro_codec.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace iceberg_avro_codec {

string ResolveAvroCodec(const string &property_value) {
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
	throw NotImplementedException(
	    "Unsupported value '%s' for 'write.manifest.compression-codec'; supported: 'gzip'/'deflate' and "
	    "'none'/'uncompressed'",
	    property_value);
}

} // namespace iceberg_avro_codec

} // namespace duckdb
