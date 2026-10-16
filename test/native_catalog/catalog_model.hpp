#pragma once

#include "catalog_json.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_util.hpp"

namespace duckdb {
namespace iceberg_test {

using Namespace = vector<string>;
using Identifier = std::pair<Namespace, string>;

struct Reply {
	int status;
	Json body;
};

class Catalog {
public:
	Catalog(FileSystem &fs, string root);
	unique_ptr<HTTPResponse> Request(const string &method, const string &path, const string &body);
	const string root;

private:
	Reply Route(const string &method, const string &path, const Json &body);
	Json GetNamespace(const Namespace &ns) const;
	Json CreateNamespace(const Json &body);
	Json UpdateNamespace(const Namespace &ns, const Json &body);
	void DropNamespace(const Namespace &ns);
	Json Load(const Identifier &key) const;
	void Rename(const Json &body);
	Json Create(const Namespace &ns, const Json &body);
	Json PrepareCommit(const Identifier &key, const Json &body);
	void CheckRequirement(const Json &metadata, const Json &requirement) const;
	void Apply(Json &metadata, const Json &update, map<string, int64_t> &last_added, vector<int64_t> &added_snapshots);
	void Validate(const Json &metadata) const;
	map<Identifier, Json> Publish(const map<Identifier, Json> &candidates);
	Json WriteMetadata(const Identifier &key, Json metadata, int64_t timestamp);
	void WriteFile(const string &path, const string &content, bool append = false);

	mutex lock;
	int64_t request_start_ms = 0;
	FileSystem &fs;
	string directory;
	string warehouse;
	map<Namespace, Json> namespaces;
	map<Identifier, Json> tables;
	map<string, std::pair<Identifier, Json>> staged;
	//! Exact, test-supplied responses for exercising client protocol handling.
	map<string, Reply> responses;
};

Namespace ParseNamespace(const Json &value);
Identifier ParseIdentifier(const Json &value);
Json NamespaceJSON(const Namespace &ns);
int64_t NowMillis();

} // namespace iceberg_test
} // namespace duckdb
