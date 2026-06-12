//===----------------------------------------------------------------------===//
//                         DuckDB - Iceberg
//
// storage/statistics/iceberg_variant_statistics.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;

//! Accumulates statistics for the shredded sub-fields of a single VARIANT column.
//!
//! Finalize() builds two Iceberg "variant bounds", upper and lower, keyed with normalized JSON paths (e.g.
//! "$['person']['age']") and whose values are the typed primitive bounds.
class IcebergVariantBounds {
public:
	//! Ingest one stats entry whose path descends into the variant.
	//! full_path is the parsed (unquoted) column path, e.g. {"v","typed_value","age","typed_value"}.
	//! variant_field_start is the index of the first path element inside the variant (the name_offset
	//! returned by IcebergTableSchema::GetFromPath). col_stats is the list of (name, value) stat structs.
	void AddStatsEntry(const vector<string> &full_path, idx_t variant_field_start, const vector<Value> &col_stats);

	//! True if a shredding type and at least one shredded field bound were collected.
	bool HasBounds() const;

	//! Build the serialized parquet-variant bounds blobs (metadata + value concatenated) for the
	//! lower and upper bounds.
	bool Finalize(ClientContext &context, bool &has_lower, string &lower_blob, bool &has_upper, string &upper_blob);

private:
	struct FieldBound {
		vector<string> field_names; // empty == variant root ("$")
		bool has_min = false;
		string min_value;
		bool has_max = false;
		string max_value;
	};

	bool has_variant_type = false;
	string variant_type_str;
	vector<FieldBound> fields;
	// path of partially shredded variant values.
	// we do not record stats for partially shredded values since the types may be different
	vector<vector<string>> partial_paths;
};

//! Decodes the parquet-variant bounds blobs produced by IcebergVariantBounds back into a VARIANT Value,
//!
//! The blob is a variant bound according to https://iceberg.apache.org/spec/#bounds-for-variant
struct IcebergVariantBoundsReader {
	//! Decode a bounds blob into a VARIANT Value by using variant_bytes_to_variant
	//! Returns false (leaving result untouched) if the blob can't
	//! be decoded (e.g. the parquet extension isn't loaded, or the bytes are malformed).
	static bool Deserialize(ClientContext &context, const string_t &blob, Value &result);

	//! Re-key a decoded bounds VARIANT (whose object keys are JSON paths like "$['age']") to the plain field
	//! names used at the call site ("age", "person.age"), returning a VARIANT Value with the renamed keys.
	static bool RekeyBoundsVariant(const Value &bounds_variant, Value &result);
};

} // namespace duckdb
