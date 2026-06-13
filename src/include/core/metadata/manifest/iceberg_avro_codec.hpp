#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! Iceberg's `write.manifest.compression-codec` (Java default "gzip", which in Avro terms is the
//! "deflate" object-container codec). The Avro COPY function this extension writes manifests through
//! only emits the "null" (uncompressed) codec, so we post-process the freshly written file: parse the
//! uncompressed Avro Object Container File, re-compress each data block with raw DEFLATE (via DuckDB's
//! vendored miniz), and rewrite the container with `avro.codec=deflate`. The rewrite goes back through
//! DuckDB's FileSystem, so object stores (S3/Azure/GCS) keep working.
namespace iceberg_avro_codec {

//! Parse `write.manifest.compression-codec` into the Avro codec name actually emitted. Returns
//! "deflate" for "gzip"/"deflate", "null"/"" for uncompressed. Anything else throws (we do not
//! silently write an unrequested codec). Default when the property is unset matches Java: "deflate".
//!
//! `catalog_allows_deletes` gates compression: emitting a compressed manifest requires writing a
//! temporary uncompressed file and deleting it afterward (the Avro COPY function cannot emit a codec
//! directly). Catalogs that forbid client deletes and manage their own storage (e.g. AWS S3 Tables,
//! allows_deletes == false) cannot delete that temp, so for them we force "null" (write uncompressed
//! directly, no temp). Those catalogs run their own compaction/GC, so uncompressed manifests are fine.
string ResolveAvroCodec(const string &property_value, bool catalog_allows_deletes);

//! Returns true if `avro_codec` denotes a compressing codec that requires the post-processing pass.
bool RequiresRecompression(const string &avro_codec);

//! Read the uncompressed Avro Object Container File at `source_path`, recompress every data block
//! with the given codec ("deflate"), and write the result to `dest_path` as a brand-new file. The
//! two paths must differ; writing a fresh `dest_path` (never reopening it for overwrite) keeps this
//! working on object stores (S3/Azure/GCS), which do not support truncating or rewriting an existing
//! object. The caller is responsible for choosing/cleaning up the temporary `source_path`. Reads and
//! writes through `context`'s FileSystem. Throws on a malformed container (it only ever processes
//! files this extension just wrote, so a failure indicates a real bug, not untrusted input).
//! Returns the number of bytes written to `dest_path`, so the caller need not issue a separate
//! size-probe (HEAD) request -- this mirrors Java's `ManifestWriter.length()`.
idx_t RecompressManifestFile(ClientContext &context, const string &source_path, const string &dest_path,
                             const string &avro_codec);

} // namespace iceberg_avro_codec

} // namespace duckdb
