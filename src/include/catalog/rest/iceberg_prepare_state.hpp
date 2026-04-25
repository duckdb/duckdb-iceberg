#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

struct IcebergPrepareContextState : public ClientContextState {
	static constexpr const char *KEY = "iceberg_prepare_context";

	bool CanRequestRebind() override {
		return true;
	}

	void RequireRebindOnPrepare() {
		require_rebind_on_prepare = true;
	}

	RebindQueryInfo OnFinalizePrepare(ClientContext &, PreparedStatementData &prepared_statement,
	                                  PreparedStatementMode mode) override {
		if (mode == PreparedStatementMode::PREPARE_ONLY && require_rebind_on_prepare) {
			prepared_statement.properties.always_require_rebind = true;
		}
		require_rebind_on_prepare = false;
		return RebindQueryInfo::DO_NOT_REBIND;
	}

private:
	bool require_rebind_on_prepare = false;
};

} // namespace duckdb
