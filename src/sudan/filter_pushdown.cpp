#include "filter_pushdown.hpp"
#include <sstream>

namespace sudan {

std::string EncodeWorldBankYearFilter(const FilterResult &filter) {
	if (!filter.has_year_filter) {
		return "";
	}
	std::ostringstream ss;
	ss << "date=";
	if (filter.year_start > 0 && filter.year_end > 0) {
		ss << filter.year_start << ":" << filter.year_end;
	} else if (filter.year_start > 0) {
		ss << filter.year_start << ":2100";
	} else if (filter.year_end > 0) {
		ss << "1900:" << filter.year_end;
	}
	return ss.str();
}

std::string EncodeWHOYearFilter(const FilterResult &filter) {
	if (!filter.has_year_filter) {
		return "";
	}
	std::ostringstream ss;
	ss << "$filter=";
	bool need_and = false;
	if (filter.year_start > 0) {
		ss << "TimeDim ge " << filter.year_start;
		need_and = true;
	}
	if (filter.year_end > 0) {
		if (need_and) {
			ss << " and ";
		}
		ss << "TimeDim le " << filter.year_end;
	}
	return ss.str();
}

std::string EncodeFAOYearFilter(const FilterResult &filter) {
	if (!filter.has_year_filter) {
		return "";
	}
	std::ostringstream ss;
	bool need_amp = false;
	if (filter.year_start > 0) {
		ss << "year_start=" << filter.year_start;
		need_amp = true;
	}
	if (filter.year_end > 0) {
		if (need_amp) {
			ss << "&";
		}
		ss << "year_end=" << filter.year_end;
	}
	return ss.str();
}

std::string EncodeUNHCRYearFilter(const FilterResult &filter) {
	if (!filter.has_year_filter) {
		return "";
	}
	std::ostringstream ss;
	bool need_amp = false;
	if (filter.year_start > 0) {
		ss << "yearFrom=" << filter.year_start;
		need_amp = true;
	}
	if (filter.year_end > 0) {
		if (need_amp) {
			ss << "&";
		}
		ss << "yearTo=" << filter.year_end;
	}
	return ss.str();
}

std::string EncodeILOYearFilter(const FilterResult &filter) {
	if (!filter.has_year_filter) {
		return "";
	}
	std::ostringstream ss;
	bool need_amp = false;
	if (filter.year_start > 0) {
		ss << "startPeriod=" << filter.year_start;
		need_amp = true;
	}
	if (filter.year_end > 0) {
		if (need_amp) {
			ss << "&";
		}
		ss << "endPeriod=" << filter.year_end;
	}
	return ss.str();
}

} // namespace sudan
