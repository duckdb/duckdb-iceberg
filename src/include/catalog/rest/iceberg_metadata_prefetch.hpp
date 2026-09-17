#pragma once

#include "catalog/rest/api/catalog_api.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

struct IcebergTable;

using IcebergLoadTableResult = APIResult<unique_ptr<const rest_api_objects::LoadTableResult>>;

//! Transaction-owned metadata for tables discovered by catalog scans. Requests run in bounded, fully drained batches;
//! workers only fetch responses, leaving catalog and transaction publication to the caller.
class IcebergMetadataPrefetch {
public:
	void Register(shared_ptr<IcebergTable> table);
	void Prefetch(ClientContext &context, const string &table_key);
	unique_ptr<IcebergLoadTableResult> Take(const string &table_key);

private:
	struct Entry {
		explicit Entry(shared_ptr<IcebergTable> table) : table(std::move(table)) {
		}
		shared_ptr<IcebergTable> table;
		unique_ptr<IcebergLoadTableResult> result;
		ErrorData error;
		bool requested = false;
	};
	class FetchTask;
	mutex lock;
	case_insensitive_map_t<unique_ptr<Entry>> entries;
	vector<reference<Entry>> pending;
	idx_t next_pending = 0;
};

} // namespace duckdb
