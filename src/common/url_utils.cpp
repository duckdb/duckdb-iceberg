#include "url_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void IRCEndpointBuilder::AddPathComponent(const string &component) {
	if (!component.empty()) {
		path_components.push_back(component);
	}
}

void IRCEndpointBuilder::SetPrefix(const string &prefix_) {
	prefix = prefix_;
}

string IRCEndpointBuilder::GetHost() const {
	return host;
}

void IRCEndpointBuilder::SetVersion(const string &version_) {
	version = version_;
}

string IRCEndpointBuilder::GetVersion() const {
	return version;
}

void IRCEndpointBuilder::SetHost(const string &host_) {
	host = host_;
}

string IRCEndpointBuilder::GetPrefix() const {
	return prefix;
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

const std::unordered_map<string, string> IRCEndpointBuilder::GetParams() {
	return params;
}

string IRCEndpointBuilder::GetURL() const {
	string ret = host;
	if (!version.empty()) {
		ret = ret + "/" + version;
	}
	// usually the warehouse is the prefix.
	if (!prefix.empty()) {
		ret = ret + "/" + prefix;
	}
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
