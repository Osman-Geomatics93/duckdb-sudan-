#pragma once

#include "duckdb.hpp"

// Use httplib directly for full HTTP method support
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

//! Struct to hold HTTP settings extracted from context (thread-safe to pass to workers)
struct HttpSettings {
	uint64_t timeout;
	bool keep_alive;
	string proxy;
	string proxy_username;
	string proxy_password;
	string user_agent;
	uint64_t max_concurrency;
	bool use_cache;
	bool follow_redirects;
};

//! Struct to hold HTTP response
struct HttpResponseData {
	int32_t status_code;
	string content_type;
	int64_t content_length;
	vector<Value> header_keys;
	vector<Value> header_values;
	vector<Value> cookies;
	string body;
	string error; // Non-empty if request failed
};

//! Represents an HTTP request
struct HttpClient {

	// Extract HTTP settings from context
	static HttpSettings ExtractHttpSettings(ClientContext &context, const string &url);

	// Execute HTTP request with given settings
	static HttpResponseData ExecuteHttpRequest(const HttpSettings &settings, const string &url, const string &method,
	                                           const duckdb_httplib_openssl::Headers &headers,
	                                           const string &request_body, const string &content_type);

	// Convenience: Execute a GET request and return body
	static HttpResponseData Get(ClientContext &context, const string &url);

	// Convenience: Execute a GET request with pre-extracted settings
	static HttpResponseData Get(const HttpSettings &settings, const string &url);
};

} // namespace duckdb
