#pragma once

namespace duckdb {

class ExtensionLoader;

struct WorldBankFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
