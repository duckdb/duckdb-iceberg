#include "catalog_model.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>

namespace duckdb {
namespace iceberg_test {

static const vector<string> ENDPOINTS {"GET /v1/{prefix}/namespaces",
                                       "POST /v1/{prefix}/namespaces",
                                       "GET /v1/{prefix}/namespaces/{namespace}",
                                       "DELETE /v1/{prefix}/namespaces/{namespace}",
                                       "POST /v1/{prefix}/namespaces/{namespace}/properties",
                                       "POST /v1/{prefix}/tables/rename",
                                       "POST /v1/{prefix}/transactions/commit",
                                       "GET /v1/{prefix}/namespaces/{namespace}/tables",
                                       "POST /v1/{prefix}/namespaces/{namespace}/tables",
                                       "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                                       "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                                       "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"};

Reply Catalog::Route(const string &method, const string &path, const Json &body) {
	if (path == "/__test/response" && method == "POST") {
		auto response_method = StringUtil::Upper(body.At("method").String());
		auto response_path = body.At("path").String();
		auto status = body.At("status").Integer();
		if (response_path.empty() || response_path[0] != '/' || status < 200 || status > 599) {
			Invalid("Scripted responses require an absolute path and an HTTP status from 200 to 599");
		}
		responses[response_method + " " + response_path] = {static_cast<int>(status), body.At("body")};
		return {204, Json()};
	}
	if (path == "/__test/response" && method == "DELETE") {
		responses.clear();
		return {204, Json()};
	}
	auto response = responses.find(method + " " + path);
	if (response != responses.end()) {
		return response->second;
	}
	auto query_start = path.find('?');
	auto parts = StringUtil::Split(path.substr(1, query_start == string::npos ? string::npos : query_start - 1), '/');
	for (auto &part : parts) {
		part = StringUtil::URLDecode(part);
	}
	map<string, string> query;
	if (query_start != string::npos) {
		for (auto &entry : StringUtil::Split(path.substr(query_start + 1), '&')) {
			auto equal = entry.find('=');
			query.emplace(StringUtil::URLDecode(entry.substr(0, equal), true),
			              equal == string::npos ? "" : StringUtil::URLDecode(entry.substr(equal + 1), true));
		}
	}
	if (parts == vector<string> {"v1", "config"} && method == "GET") {
		auto endpoints = Json::Array();
		for (auto &endpoint : ENDPOINTS) {
			endpoints.Push(endpoint);
		}
		return {200,
		        Json::Object({{"defaults", Json::Object()}, {"overrides", Json::Object()}, {"endpoints", endpoints}})};
	}
	if (parts == vector<string> {"v1", "tables", "rename"} && method == "POST") {
		Rename(body);
		return {204, Json()};
	}
	if (parts == vector<string> {"v1", "transactions", "commit"} && method == "POST") {
		map<Identifier, Json> candidates;
		for (auto &change : body.At("table-changes").Items()) {
			auto key = ParseIdentifier(change.At("identifier"));
			if (candidates.count(key)) {
				Invalid("Duplicate table identifier in transaction");
			}
			candidates.emplace(key, PrepareCommit(key, change));
		}
		Publish(candidates);
		return {204, Json()};
	}
	if (parts.size() < 2 || parts[0] != "v1" || parts[1] != "namespaces") {
		Unsupported("Unknown route: " + method + " " + path);
	}
	if (parts.size() == 2) {
		if (method == "GET") {
			Namespace parent;
			if (query.count("parent")) {
				parent = StringUtil::Split(query.at("parent"), '\x1f');
			}
			auto result = Json::Array();
			for (auto &entry : namespaces) {
				if (entry.first.size() == parent.size() + 1 &&
				    std::equal(parent.begin(), parent.end(), entry.first.begin())) {
					result.Push(NamespaceJSON(entry.first));
				}
			}
			return {200, Json::Object({{"namespaces", result}})};
		}
		if (method == "POST") {
			return {200, CreateNamespace(body)};
		}
	}
	if (parts.size() >= 3) {
		auto ns = StringUtil::Split(parts[2], '\x1f');
		if (parts.size() == 3) {
			if (method == "GET") {
				return {200, GetNamespace(ns)};
			}
			if (method == "DELETE") {
				DropNamespace(ns);
				return {204, Json()};
			}
		}
		if (parts.size() == 4 && parts[3] == "properties" && method == "POST") {
			return {200, UpdateNamespace(ns, body)};
		}
		if (parts.size() == 4 && parts[3] == "tables") {
			GetNamespace(ns);
			if (method == "GET") {
				auto result = Json::Array();
				for (auto &entry : tables) {
					if (entry.first.first == ns) {
						result.Push(Json::Object({{"namespace", NamespaceJSON(ns)}, {"name", entry.first.second}}));
					}
				}
				return {200, Json::Object({{"identifiers", result}})};
			}
			if (method == "POST") {
				return {200, Create(ns, body)};
			}
		}
		if (parts.size() == 5 && parts[3] == "tables") {
			Identifier key {ns, parts[4]};
			if (method == "GET") {
				return {200, Load(key)};
			}
			if (method == "POST") {
				return {200, Publish({{key, PrepareCommit(key, body)}}).at(key)};
			}
			if (method == "DELETE") {
				if (query.count("purgeRequested") && StringUtil::Lower(query.at("purgeRequested")) != "false") {
					Unsupported("Purge is not implemented; normal drop only unregisters");
				}
				Load(key);
				tables.erase(key);
				return {204, Json()};
			}
		}
	}
	Unsupported("Unknown route: " + method + " " + path);
}

unique_ptr<HTTPResponse> Catalog::Request(const string &method, const string &path, const string &body_text) {
	lock_guard<mutex> guard(lock);
	request_start_ms = NowMillis();
	Json body;
	Reply reply;
	string internal_error;
	try {
		if (body_text.size() > 16 * 1024 * 1024) {
			Invalid("Request body exceeds 16 MiB");
		}
		// Scripted routes can model non-JSON protocols too (e.g. OAuth2 form bodies).
		if (!body_text.empty() && !responses.count(method + " " + path)) {
			body = Json::Parse(body_text);
			if (!body.IsObject()) {
				Invalid("Request body must be a JSON object");
			}
		}
		reply = Route(method, path, body);
	} catch (const CatalogError &error) {
		reply = {error.code, Json::Object({{"error", Json::Object({{"message", error.what()},
		                                                           {"type", error.kind},
		                                                           {"code", int64_t(error.code)}})}})};
	} catch (const std::exception &error) {
		internal_error = error.what();
		reply = {500, Json::Object({{"error", Json::Object({{"message", "Native mock catalog exception"},
		                                                    {"type", "ServerError"},
		                                                    {"code", int64_t(500)}})}})};
	}
	auto response = make_uniq<HTTPResponse>(HTTPUtil::ToStatusCode(reply.status));
	response->body = reply.body.IsNull() ? "" : reply.body.Dump();
	response->headers.Insert("Content-Type", "application/json");
	response->headers.Insert("Content-Length", std::to_string(response->body.size()));
	response->success = reply.status >= 200 && reply.status < 300;
	auto truncate = [](const string &text) {
		return text.size() <= 16384 ? text : text.substr(0, 16384) + "... [truncated]";
	};
	auto event = Json::Object({{"method", method},
	                           {"path", path},
	                           {"status", int64_t(reply.status)},
	                           {"request", truncate(body_text)},
	                           {"response", truncate(response->body)}});
	if (!internal_error.empty()) {
		event["exception"] = truncate(internal_error);
	}
	// Diagnostics must not turn an already published commit into a failed response.
	try {
		WriteFile(fs.JoinPath(directory, "requests.jsonl"), event.Dump() + "\n", true);
	} catch (const std::exception &) {
	}
	if (method == "HEAD") {
		response->body.clear();
	}
	return response;
}

} // namespace iceberg_test
} // namespace duckdb
