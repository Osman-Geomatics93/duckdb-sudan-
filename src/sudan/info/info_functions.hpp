#pragma once

namespace duckdb {

class ExtensionLoader;

struct InfoFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
