#include "cache.hpp"

namespace sudan {

std::string ResponseCache::Get(const std::string &url) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = cache_.find(url);
	if (it == cache_.end()) {
		return "";
	}
	auto now = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.timestamp).count();
	if (elapsed > CACHE_TTL_SECONDS) {
		cache_.erase(it);
		return "";
	}
	return it->second.body;
}

void ResponseCache::Put(const std::string &url, const std::string &body) {
	std::lock_guard<std::mutex> lock(mutex_);
	CacheEntry entry;
	entry.body = body;
	entry.timestamp = std::chrono::steady_clock::now();
	cache_[url] = std::move(entry);
}

void ResponseCache::Clear() {
	std::lock_guard<std::mutex> lock(mutex_);
	cache_.clear();
}

ResponseCache &ResponseCache::Instance() {
	static ResponseCache instance;
	return instance;
}

} // namespace sudan
