#include "url_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void IRCEndpointBuilder::AddPathComponent(const string &component) {
	if (!component.empty()) {
		path_components.push_back(component);
	}
}

string IRCEndpointBuilder::GetHost() const {
	return host;
}

void IRCEndpointBuilder::SetHost(const string &host_) {
	host = host_;
}

void IRCEndpointBuilder::SetParam(const string &key, const string &value) {
	params[key] = value;
}

string IRCEndpointBuilder::GetParam(const string &key) const {
	if (params.find(key) != params.end()) {
		return params.at(key);
	}
	return "";
}

const std::unordered_map<string, string> IRCEndpointBuilder::GetParams() const {
	return params;
}

string IRCEndpointBuilder::GetURL() const {
	//! {host}[/{version}][/{prefix}]/{path_component[0]}/{path_component[1]}
	string ret = host;
	for (auto &component : path_components) {
		ret += "/" + component;
	}

	// encode params
	auto sep = "?";
	if (params.size() > 0) {
		for (auto &param : params) {
			auto key = StringUtil::URLEncode(param.first);
			auto value = StringUtil::URLEncode(param.second);
			ret += sep + key + "=" + value;
			sep = "&";
		}
	}
	return ret;
}

} // namespace duckdb
