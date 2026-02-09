#include "providers.hpp"
#include <algorithm>

namespace sudan {

const CountryInfo *FindCountryByISO3(const std::string &iso3) {
	for (const auto &country : SUPPORTED_COUNTRIES) {
		if (country.iso3 == iso3) {
			return &country;
		}
	}
	return nullptr;
}

const Provider *FindProvider(const std::string &id) {
	for (const auto &provider : PROVIDERS) {
		if (provider.id == id) {
			return &provider;
		}
	}
	return nullptr;
}

bool ValidateCountryCodes(const std::vector<std::string> &codes) {
	for (const auto &code : codes) {
		bool found = false;
		for (const auto &country : SUPPORTED_COUNTRIES) {
			if (country.iso3 == code || country.iso2 == code) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

std::string NormalizeCountryCode(const std::string &code) {
	for (const auto &country : SUPPORTED_COUNTRIES) {
		if (country.iso3 == code) {
			return country.iso3;
		}
		if (country.iso2 == code) {
			return country.iso3;
		}
	}
	return code;
}

} // namespace sudan
