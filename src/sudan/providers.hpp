#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>

namespace sudan {

//! API Provider for Sudan data
struct Provider {
	std::string id;          // "worldbank", "who", "fao", "unhcr", "ilo"
	std::string name;        // "World Bank"
	std::string name_ar;     // Arabic name
	std::string description;
	std::string base_url;
	std::string country_param; // "SDN", "SD", or "Sudan" depending on API
};

//! Supported country info (Sudan + neighbors)
struct CountryInfo {
	std::string iso3;    // "SDN"
	std::string iso2;    // "SD"
	std::string name;    // "Sudan"
	std::string name_ar; // Arabic name
};

//! Supported countries: Sudan + neighbors
static const std::vector<CountryInfo> SUPPORTED_COUNTRIES = {
    {"SDN", "SD", "Sudan", "\xd8\xa7\xd9\x84\xd8\xb3\xd9\x88\xd8\xaf\xd8\xa7\xd9\x86"},
    {"EGY", "EG", "Egypt", "\xd9\x85\xd8\xb5\xd8\xb1"},
    {"ETH", "ET", "Ethiopia", "\xd8\xa5\xd8\xab\xd9\x8a\xd9\x88\xd8\xa8\xd9\x8a\xd8\xa7"},
    {"TCD", "TD", "Chad", "\xd8\xaa\xd8\xb4\xd8\xa7\xd8\xaf"},
    {"SSD", "SS", "South Sudan", "\xd8\xac\xd9\x86\xd9\x88\xd8\xa8 \xd8\xa7\xd9\x84\xd8\xb3\xd9\x88\xd8\xaf\xd8\xa7\xd9\x86"},
    {"ERI", "ER", "Eritrea", "\xd8\xa5\xd8\xb1\xd9\x8a\xd8\xaa\xd8\xb1\xd9\x8a\xd8\xa7"},
    {"LBY", "LY", "Libya", "\xd9\x84\xd9\x8a\xd8\xa8\xd9\x8a\xd8\xa7"},
    {"CAF", "CF", "Central African Republic",
     "\xd8\xac\xd9\x85\xd9\x87\xd9\x88\xd8\xb1\xd9\x8a\xd8\xa9 \xd8\xa3\xd9\x81\xd8\xb1\xd9\x8a\xd9\x82\xd9\x8a\xd8\xa7 \xd8\xa7\xd9\x84\xd9\x88\xd8\xb3\xd8\xb7\xd9\x89"},
};

//! API Providers
static const std::vector<Provider> PROVIDERS = {
    {"worldbank", "World Bank",
     "\xd8\xa7\xd9\x84\xd8\xa8\xd9\x86\xd9\x83 \xd8\xa7\xd9\x84\xd8\xaf\xd9\x88\xd9\x84\xd9\x8a",
     "World Development Indicators and other World Bank datasets",
     "https://api.worldbank.org/v2/", "SDN"},
    {"who", "World Health Organization",
     "\xd9\x85\xd9\x86\xd8\xb8\xd9\x85\xd8\xa9 \xd8\xa7\xd9\x84\xd8\xb5\xd8\xad\xd8\xa9 \xd8\xa7\xd9\x84\xd8\xb9\xd8\xa7\xd9\x84\xd9\x85\xd9\x8a\xd8\xa9",
     "Global Health Observatory (GHO) data",
     "https://ghoapi.azureedge.net/api/", "SDN"},
    {"fao", "Food and Agriculture Organization",
     "\xd9\x85\xd9\x86\xd8\xb8\xd9\x85\xd8\xa9 \xd8\xa7\xd9\x84\xd8\xa3\xd8\xba\xd8\xb0\xd9\x8a\xd8\xa9 \xd9\x88\xd8\xa7\xd9\x84\xd8\xb2\xd8\xb1\xd8\xa7\xd8\xb9\xd8\xa9",
     "FAOSTAT agricultural statistics",
     "https://fenixservices.fao.org/faostat/api/v1/", "276"},
    {"unhcr", "UNHCR",
     "\xd8\xa7\xd9\x84\xd9\x85\xd9\x81\xd9\x88\xd8\xb6\xd9\x8a\xd8\xa9 \xd8\xa7\xd9\x84\xd8\xb3\xd8\xa7\xd9\x85\xd9\x8a\xd8\xa9",
     "UN Refugee Agency displacement and population data",
     "https://api.unhcr.org/population/v1/", "SDN"},
    {"ilo", "International Labour Organization",
     "\xd9\x85\xd9\x86\xd8\xb8\xd9\x85\xd8\xa9 \xd8\xa7\xd9\x84\xd8\xb9\xd9\x85\xd9\x84 \xd8\xa7\xd9\x84\xd8\xaf\xd9\x88\xd9\x84\xd9\x8a\xd8\xa9",
     "International Labour Organization statistics",
     "https://www.ilo.org/ilostat/api/v1/", "SDN"},
};

//! Lookup a country by ISO3 code
const CountryInfo *FindCountryByISO3(const std::string &iso3);

//! Lookup a provider by ID
const Provider *FindProvider(const std::string &id);

//! Validate a list of country codes, returns true if all valid
bool ValidateCountryCodes(const std::vector<std::string> &codes);

//! Get the ISO3 code for a given ISO2 or ISO3 code
std::string NormalizeCountryCode(const std::string &code);

} // namespace sudan
