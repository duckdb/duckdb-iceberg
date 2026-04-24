#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"

namespace duckdb {

class IcebergTransaction;

enum class IcebergTransactionUpdateType : uint8_t { ALTER, DELETE, RENAME };

struct IcebergTransactionUpdate {
public:
	IcebergTransactionUpdate(IcebergTransaction &transaction, IcebergTransactionUpdateType type);
	virtual ~IcebergTransactionUpdate();

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException(
			    "Failed to cast IcebergTransactionUpdate to type - IcebergTransactionUpdate type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException(
			    "Failed to cast IcebergTransactionUpdate to type - IcebergTransactionUpdate type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	IcebergTransaction &transaction;
	IcebergTransactionUpdateType type;
};

//! Update a table with a regular alter
struct IcebergTransactionAlterUpdate : public IcebergTransactionUpdate {
public:
	static constexpr const IcebergTransactionUpdateType TYPE = IcebergTransactionUpdateType::ALTER;

public:
	IcebergTransactionAlterUpdate(IcebergTransaction &transaction);
	virtual ~IcebergTransactionAlterUpdate() override;

public:
	IcebergTableInformation &CreateTable(const string &table_key, IcebergTableInformation &&table);
	IcebergTableInformation &GetOrInitializeTable(const IcebergTableInformation &table);
	bool HasUpdates() const;
	//! All the tables touched in this atomic block
	case_insensitive_map_t<IcebergTableInformation> updated_tables;
	//! The tables successively committed (used if multi-table commit isn't available)
	case_insensitive_set_t committed_tables;
};

//! Drop a table
struct IcebergTransactionDeleteUpdate : public IcebergTransactionUpdate {
public:
	static constexpr const IcebergTransactionUpdateType TYPE = IcebergTransactionUpdateType::DELETE;

public:
	IcebergTransactionDeleteUpdate(IcebergTransaction &transaction, const IcebergTableInformation &table);
	virtual ~IcebergTransactionDeleteUpdate() override;

public:
	IcebergTableInformation deleted_table;
};

//! Rename a table
struct IcebergTransactionRenameUpdate : public IcebergTransactionUpdate {
public:
	static constexpr const IcebergTransactionUpdateType TYPE = IcebergTransactionUpdateType::RENAME;

public:
	IcebergTransactionRenameUpdate(IcebergTransaction &transaction, const IcebergTableInformation &table,
	                               const string &new_name);
	virtual ~IcebergTransactionRenameUpdate() override;

public:
	const IcebergTableInformation &table;
	IcebergTableInformation new_table;
	string new_name;
};

} // namespace duckdb
