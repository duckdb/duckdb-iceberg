#include "catalog/iceberg_catalog_types.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

IcebergNamespace IcebergNamespace::Parse(const string &schema_name) {
	idx_t start = 0;
	idx_t end = schema_name.find(".", start);
	vector<string> levels;
	while (end != std::string::npos) {
		levels.push_back(schema_name.substr(start, end - start));
		start = end + 1;
		end = schema_name.find(".", start);
	}
	levels.push_back(schema_name.substr(start, end - start));
	return IcebergNamespace(std::move(levels));
}

string IcebergNamespace::ToString() const {
	return StringUtil::Join(levels, ".");
}

IcebergCatalogError::IcebergCatalogError(IcebergCatalogStatus status, ErrorData error_p, string error_type_p)
    : status(status), error(std::move(error_p)), error_type(std::move(error_type_p)) {
}

void IcebergCatalogError::Throw() const {
	error.Throw();
	throw InternalException("IcebergCatalogError::Throw did not throw");
}

IcebergLoadedTable::IcebergLoadedTable(IcebergTableAccess access_p) : access(std::move(access_p)) {
}

IcebergLoadedTable::~IcebergLoadedTable() {
}

IcebergTableLoad::IcebergTableLoad(IcebergEntryId id_p) : id(std::move(id_p)) {
}

IcebergTableLoad::~IcebergTableLoad() {
}

bool IcebergTableLoad::Publish(shared_ptr<const IcebergLoadedTable> table) {
	return false;
}

} // namespace duckdb
