#include "core/deletes/iceberg_positional_delete.hpp"

namespace duckdb {

unique_ptr<DeleteFilter> IcebergPositionalDeleteData::ToFilter() const {
	return make_uniq<IcebergPositionalDeleteFilter>(shared_from_this());
}

void IcebergPositionalDeleteData::ToSet(set<idx_t> &out) const {
	roaring::api::roaring64_bitmap_iterate(
	    invalid_rows.get(),
	    [](uint64_t row, void *context) {
		    static_cast<set<idx_t> *>(context)->insert(row);
		    return true;
	    },
	    &out);
}

idx_t IcebergPositionalDeleteFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	if (count == 0) {
		return 0;
	}
	result_sel.Initialize(count);
	idx_t selection_idx = 0;
	idx_t offset = 0;
	const auto start = static_cast<uint64_t>(start_row_index);
	// Seek independently for each range: filters can be consumed out of order or
	// concurrently, while the shared bitmap remains immutable.
	using namespace roaring::api;
	unique_ptr<roaring64_iterator_t, decltype(&roaring64_iterator_free)> deleted(
	    roaring64_iterator_create(data->invalid_rows.get()), roaring64_iterator_free);
	if (!deleted) {
		throw OutOfMemoryException("Failed to allocate Iceberg positional delete iterator");
	}
	roaring64_iterator_move_equalorlarger(deleted.get(), start);
	constexpr idx_t BATCH_SIZE = 256;
	uint64_t positions[BATCH_SIZE];
	while (roaring64_iterator_has_value(deleted.get()) && roaring64_iterator_value(deleted.get()) - start < count) {
		const auto read = roaring64_iterator_read(deleted.get(), positions, BATCH_SIZE);
		for (idx_t i = 0; i < read && positions[i] - start < count; ++i) {
			const auto deleted_offset = positions[i] - start;
			while (offset < deleted_offset) {
				result_sel.set_index(selection_idx++, offset++);
			}
			offset = deleted_offset + 1;
		}
	}
	while (offset < count) {
		result_sel.set_index(selection_idx++, offset++);
	}
	return selection_idx;
}

} // namespace duckdb
