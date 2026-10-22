#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! Schemas created in a transaction, whose namespaces are only created on the server at commit
class IcebergPendingSchemaCreates {
public:
	bool Contains(const string &name) const {
		return schemas.count(name) > 0;
	}
	bool Empty() const {
		return schemas.empty();
	}
	void Clear() {
		schemas.clear();
	}

	//! Returns true if the schema is already pending, throws if that is a conflict for `on_conflict`
	bool CheckPending(const string &name, OnCreateConflict on_conflict) const {
		if (!Contains(name)) {
			return false;
		}
		if (on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
			ThrowAlreadyExists(name);
		}
		return true;
	}

	void Add(const string &name, OnCreateConflict on_conflict) {
		D_ASSERT(!Contains(name));
		D_ASSERT(on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT);
		schemas.emplace(name, on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
	}

	void OnNamespaceExists(const string &name) const {
		auto entry = schemas.find(name);
		D_ASSERT(entry != schemas.end());
		if (!entry->second) {
			ThrowAlreadyExists(name);
		}
	}

	unordered_map<string, bool>::const_iterator begin() const {
		return schemas.begin();
	}
	unordered_map<string, bool>::const_iterator end() const {
		return schemas.end();
	}

private:
	static void ThrowAlreadyExists(const string &name) {
		throw CatalogException("Schema with name \"%s\" already exists", name);
	}

private:
	//! schema name -> whether an existing namespace is acceptable (IF NOT EXISTS)
	unordered_map<string, bool> schemas;
};

} // namespace duckdb
