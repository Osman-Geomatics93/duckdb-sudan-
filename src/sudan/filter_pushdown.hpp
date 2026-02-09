#pragma once

#include <string>
#include <vector>

namespace sudan {

//! Filter pushdown result for a provider query
struct FilterResult {
	bool has_year_filter = false;
	int32_t year_start = -1;
	int32_t year_end = -1;
};

//! Encode year range filter as World Bank API parameter
//! Returns e.g. "date=2010:2023"
std::string EncodeWorldBankYearFilter(const FilterResult &filter);

//! Encode year range filter as WHO GHO OData filter
//! Returns e.g. "$filter=TimeDim ge 2015 and TimeDim le 2023"
std::string EncodeWHOYearFilter(const FilterResult &filter);

//! Encode year range filter as FAO API parameters
//! Returns e.g. "year_start=2010&year_end=2023"
std::string EncodeFAOYearFilter(const FilterResult &filter);

//! Encode year range filter as UNHCR API parameters
//! Returns e.g. "yearFrom=2010&yearTo=2023"
std::string EncodeUNHCRYearFilter(const FilterResult &filter);

//! Encode year range filter as ILO API parameters
//! Returns e.g. "startPeriod=2010&endPeriod=2023"
std::string EncodeILOYearFilter(const FilterResult &filter);

} // namespace sudan
