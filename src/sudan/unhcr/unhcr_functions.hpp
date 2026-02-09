#pragma once

namespace duckdb {

class ExtensionLoader;

struct UNHCRFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
