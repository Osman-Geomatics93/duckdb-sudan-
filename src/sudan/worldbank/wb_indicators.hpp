#pragma once

namespace duckdb {

class ExtensionLoader;

struct WorldBankIndicatorFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
