#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <chrono>

namespace sudan {

//! Simple in-memory response cache for API responses within a session
class ResponseCache {
public:
	struct CacheEntry {
		std::string body;
		std::chrono::steady_clock::time_point timestamp;
	};

	//! Get a cached response for the given URL. Returns empty string if not found or expired.
	std::string Get(const std::string &url);

	//! Store a response in the cache
	void Put(const std::string &url, const std::string &body);

	//! Clear the cache
	void Clear();

	//! Get the singleton instance
	static ResponseCache &Instance();

private:
	std::unordered_map<std::string, CacheEntry> cache_;
	std::mutex mutex_;
	// Cache entries expire after 5 minutes
	static constexpr int CACHE_TTL_SECONDS = 300;
};

} // namespace sudan
