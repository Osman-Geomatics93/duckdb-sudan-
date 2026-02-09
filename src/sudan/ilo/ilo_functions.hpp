#pragma once

namespace duckdb {

class ExtensionLoader;

struct ILOFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
