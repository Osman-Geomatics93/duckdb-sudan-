#pragma once

namespace duckdb {

class ExtensionLoader;

struct GeoFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
