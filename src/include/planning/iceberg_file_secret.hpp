#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! The secret selected for one read, independent of immutable metadata and its cache lifetime.
struct IcebergFileSecret {
	IcebergFileSecret() = default;
	explicit IcebergFileSecret(string name) : value(name.empty() ? Value() : Value(std::move(name))) {
	}

	void Apply(OpenFileInfo &file) const {
		if (value.IsNull()) {
			return;
		}
		if (!file.extended_info) {
			file.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		}
		file.extended_info->options[ExtendedOpenFileInfo::SECRET_OPTION] = value;
	}

private:
	Value value;
};

} // namespace duckdb
