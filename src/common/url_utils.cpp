#include "url_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

IRCEndpointBuilder::IRCEndpointBuilder() {
}

string AddHttpHostIfMissing(const string &url) {
	auto lower_url = StringUtil::Lower(url);
	if (StringUtil::StartsWith(lower_url, "http://") || StringUtil::StartsWith(lower_url, "https://")) {
		return url;
	}
	return "http://" + url;
}

void IRCEndpointBuilder::AddPathComponent(IRCPathComponent &&component) {
	if (component.raw.empty()) {
		return;
	}
	path_components.push_back(std::move(component));
}

void IRCEndpointBuilder::AddPrefixComponent(const string &component, const bool &prefix_is_one_component) {
	if (component.empty()) {
		return;
	}

	// If the component contains slashes, split it into multiple segments
	if (!prefix_is_one_component && component.find('/') != string::npos) {
		auto segments = StringUtil::Split(component, '/');
		for (const auto &segment : segments) {
			if (!segment.empty()) {
				AddPathComponent(IRCPathComponent::RegularComponent(segment));
			}
		}
	} else {
		// Single component without slashes
		AddPathComponent(IRCPathComponent::RegularComponent(component));
	}
}

string IRCEndpointBuilder::GetHost() const {
	return host;
}

void IRCEndpointBuilder::SetHost(const string &host_) {
	host = host_;
}

void IRCEndpointBuilder::SetParam(const string &key, IRCPathComponent &&param) {
	params.emplace(key, std::move(param));
}

const unordered_map<string, IRCPathComponent> &IRCEndpointBuilder::GetParams() const {
	return params;
}

string IRCEndpointBuilder::GetURLEncoded() const {
	//! {host}[/{version}][/{prefix}]/{path_component[0]}/{path_component[1]}
	string ret = host;
	for (auto &component : path_components) {
		ret += "/" + component.encoded;
	}

	// encode params
	auto sep = "?";
	if (params.size() > 0) {
		for (auto &it : params) {
			auto key = StringUtil::URLEncode(it.first);
			auto &value = it.second.encoded;
			ret += sep + key + "=" + value;
			sep = "&";
		}
	}
	return ret;
}

IRCEndpointBuilder IRCEndpointBuilder::FromURL(const string &url) {
	auto url_with_http = AddHttpHostIfMissing(url);
	auto ret = IRCEndpointBuilder();
	size_t schemeEnd = url_with_http.find("://");
	if (schemeEnd == string::npos) {
		throw InvalidInputException("Invalid URL: missing scheme");
	}
	string scheme = url_with_http.substr(0, schemeEnd);

	// Start of host
	size_t hostStart = schemeEnd + 3;

	// Find where host ends (at first '/' after scheme)
	size_t pathStart = url_with_http.find('/', hostStart);
	ret.SetHost(url_with_http.substr(0, pathStart));

	// Extract path and split into components
	string path = url_with_http.substr(pathStart + 1);
	size_t pos = 0;
	string component = "";
	while ((pos = path.find('/')) != string::npos) {
		component = path.substr(0, pos);
		if (!component.empty()) {
			ret.AddPathComponent(IRCPathComponent::RegularComponent(component));
		}
		path.erase(0, pos + 1);
	}
	if (!path.empty()) {
		ret.AddPathComponent(IRCPathComponent::RegularComponent(path));
	}
	return ret;
}

} // namespace duckdb
