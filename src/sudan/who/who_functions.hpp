#pragma once

namespace duckdb {

class ExtensionLoader;

struct WHOFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
