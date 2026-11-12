#include "core/metadata/snapshot/iceberg_row_id_allocator.hpp"

#include "core/metadata/manifest/iceberg_pending_manifest.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/add.hpp"

namespace duckdb {

static int64_t AddRowIds(int64_t first, int64_t count) {
	int64_t end;
	if (first < 0 || count < 0 || !TryAddOperator::Operation(first, count, end)) {
		throw InvalidConfigurationException("Iceberg row ID allocation exceeds the nonnegative BIGINT range");
	}
	return end;
}

IcebergRowIdAllocator::IcebergRowIdAllocator(int64_t table_next_row_id)
    : snapshot_start(table_next_row_id), next_row_id(table_next_row_id) {
	if (table_next_row_id < 0) {
		throw InvalidConfigurationException("Iceberg next-row-id cannot be negative");
	}
}

void IcebergRowIdAllocator::AssignExistingManifest(IcebergManifest &manifest) {
	if (manifest.content != IcebergManifestContentType::DATA) {
		return;
	}
	if (!manifest.counts || !manifest.counts->added_rows_count || !manifest.counts->existing_rows_count) {
		throw InvalidConfigurationException("Manifest row counts are required to assign row IDs");
	}
	auto count = AddRowIds(NumericCast<int64_t>(*manifest.counts->added_rows_count),
	                       NumericCast<int64_t>(*manifest.counts->existing_rows_count));
	auto first = manifest.first_row_id.value_or(next_row_id);
	next_row_id = MaxValue<int64_t>(next_row_id, AddRowIds(first, count));
	manifest.first_row_id = first;
}

optional<int64_t> IcebergRowIdAllocator::AllocateManifest(const IcebergPendingManifest &manifest) {
	if (manifest.GetMetadata().content != IcebergManifestContentType::DATA) {
		return nullopt;
	}
	int64_t count = 0;
	for (const auto &entry : manifest.GetEntries()) {
		switch (entry.status) {
		case IcebergManifestEntryStatusType::ADDED:
		case IcebergManifestEntryStatusType::EXISTING:
			count = AddRowIds(count, entry.data_file.record_count);
			break;
		case IcebergManifestEntryStatusType::DELETED:
			break;
		default:
			throw InvalidConfigurationException("Invalid manifest entry status");
		}
	}
	auto first = next_row_id;
	next_row_id = AddRowIds(first, count);
	return first;
}

void IcebergRowIdAllocator::CompleteSnapshot(IcebergSnapshot &snapshot) {
	snapshot.first_row_id = snapshot_start;
	snapshot.added_rows = next_row_id - snapshot_start;
	snapshot_start = next_row_id;
}

} // namespace duckdb
