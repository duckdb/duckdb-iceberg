//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/api/url_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

string AddHttpHostIfMissing(const string &url);

//! Strip the scheme (http:// or https://) from a URL string, returning the bare host+path
string StripScheme(const string &url);

struct IRCPathComponent {
public:
	static constexpr const char *DEFAULT_NAMESPACE_SEPARATOR = "\x1f";

private:
	IRCPathComponent(const string &raw, const string &encoded) : raw(raw), encoded(encoded) {
	}

public:
	static IRCPathComponent RegularComponent(const string &raw) {
		return IRCPathComponent(raw, StringUtil::URLEncode(raw));
	}
	static IRCPathComponent NamespaceComponent(const vector<string> &items,
	                                           const string &namespace_separator = DEFAULT_NAMESPACE_SEPARATOR) {
		auto raw = StringUtil::Join(items, items.size(), namespace_separator, [](const string &item) { return item; });
		auto encoded = StringUtil::Join(items, items.size(), namespace_separator,
		                                [](const string &item) { return StringUtil::URLEncode(item); });
		return IRCPathComponent(raw, encoded);
	}

public:
	const string raw;
	const string encoded;
};

class IRCEndpointBuilder {
public:
	IRCEndpointBuilder();
	void AddPathComponent(IRCPathComponent &&component);
	void AddPrefixComponent(const string &component, const bool &prefix_is_one_component);

	void SetHost(const string &host);
	string GetHost() const;

	void SetParam(const string &key, IRCPathComponent &&param);
	const unordered_map<string, IRCPathComponent> &GetParams() const;

	string GetURLEncoded() const;
	static IRCEndpointBuilder FromURL(const string &url);

	//! path components when querying. Like namespaces/tables etc.
	vector<IRCPathComponent> path_components;

private:
	string host;
	unordered_map<string, IRCPathComponent> params;
};

} // namespace duckdb
