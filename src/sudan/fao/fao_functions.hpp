#pragma once

namespace duckdb {

class ExtensionLoader;

struct FAOFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
