#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class IcebergSchemaEntry;

struct IcebergPendingSchemaCreate {
	shared_ptr<IcebergSchemaEntry> entry;
	//! whether an existing namespace is acceptable (IF NOT EXISTS)
	bool ignore_conflict;
};

//! Schemas created in a transaction, whose namespaces are only created on the server at commit
class IcebergPendingSchemaCreates {
public:
	bool Contains(const string &name) const {
		return schemas.count(name) > 0;
	}
	bool Empty() const {
		return schemas.empty();
	}
	idx_t Size() const {
		return schemas.size();
	}

	optional_ptr<IcebergSchemaEntry> GetEntry(const string &name) const {
		auto entry = schemas.find(name);
		if (entry == schemas.end()) {
			return nullptr;
		}
		return entry->second.entry.get();
	}

	//! Returns the pending entry, or nullptr if there is none; throws if it is a conflict for `on_conflict`
	optional_ptr<IcebergSchemaEntry> CheckPending(const string &name, OnCreateConflict on_conflict) const {
		auto entry = GetEntry(name);
		if (entry && on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
			ThrowAlreadyExists(name);
		}
		return entry;
	}

	void Add(const string &name, shared_ptr<IcebergSchemaEntry> entry, OnCreateConflict on_conflict) {
		D_ASSERT(!Contains(name));
		D_ASSERT(on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT);
		schemas.emplace(
		    name, IcebergPendingSchemaCreate {std::move(entry), on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT});
	}

	void OnNamespaceExists(const string &name) const {
		auto entry = schemas.find(name);
		D_ASSERT(entry != schemas.end());
		if (!entry->second.ignore_conflict) {
			ThrowAlreadyExists(name);
		}
	}

	unordered_map<string, IcebergPendingSchemaCreate>::const_iterator begin() const {
		return schemas.begin();
	}
	unordered_map<string, IcebergPendingSchemaCreate>::const_iterator end() const {
		return schemas.end();
	}

private:
	static void ThrowAlreadyExists(const string &name) {
		throw CatalogException("Schema with name \"%s\" already exists", name);
	}

private:
	unordered_map<string, IcebergPendingSchemaCreate> schemas;
};

} // namespace duckdb
