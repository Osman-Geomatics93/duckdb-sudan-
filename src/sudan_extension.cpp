#define DUCKDB_EXTENSION_MAIN

#include "sudan_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// SUDAN
#include "sudan/worldbank/wb_functions.hpp"
#include "sudan/worldbank/wb_indicators.hpp"
#include "sudan/who/who_functions.hpp"
#include "sudan/fao/fao_functions.hpp"
#include "sudan/unhcr/unhcr_functions.hpp"
#include "sudan/ilo/ilo_functions.hpp"
#include "sudan/geo/geo_functions.hpp"
#include "sudan/info/info_functions.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register functions
	WorldBankFunctions::Register(loader);
	WorldBankIndicatorFunctions::Register(loader);
	WHOFunctions::Register(loader);
	FAOFunctions::Register(loader);
	UNHCRFunctions::Register(loader);
	ILOFunctions::Register(loader);
	GeoFunctions::Register(loader);
	InfoFunctions::Register(loader);
}

void SudanExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string SudanExtension::Name() {
	return "sudan";
}

std::string SudanExtension::Version() const {
#ifdef EXT_VERSION_SUDAN
	return EXT_VERSION_SUDAN;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sudan, loader) {
	duckdb::LoadInternal(loader);
}
}
