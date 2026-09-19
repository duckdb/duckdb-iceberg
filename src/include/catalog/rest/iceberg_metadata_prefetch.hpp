#pragma once

#include "catalog/rest/api/catalog_api.hpp"

namespace duckdb {

struct IcebergTable;

using IcebergLoadTableResult = APIResult<unique_ptr<const rest_api_objects::LoadTableResult>>;

//! Schedule metadata requests when tables are listed. Workers only fetch responses;
//! catalog and transaction publication remains on the consuming thread.
class IcebergMetadataPrefetch {
public:
	IcebergMetadataPrefetch();
	~IcebergMetadataPrefetch();
	void CancelAndDrain();
	void Register(ClientContext &context, shared_ptr<IcebergTable> table);
	unique_ptr<IcebergLoadTableResult> Take(ClientContext &context, const string &table_key);

private:
	class State;
	class QueryState;
	shared_ptr<State> state;
};

} // namespace duckdb
