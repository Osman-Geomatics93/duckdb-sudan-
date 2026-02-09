#include "wb_functions.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"
using namespace duckdb_yyjson; // NOLINT

// SUDAN
#include "sudan/providers.hpp"
#include "sudan/http_client.hpp"
#include "sudan/filter_pushdown.hpp"
#include "sudan/cache.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// SUDAN_WorldBank
//======================================================================================================================

struct SudanWorldBank {

	//! A data row from the World Bank API
	struct DataRow {
		string indicator_id;
		string indicator_name;
		string country_id;
		string country_name;
		int32_t year;
		double value;
		bool has_value;
	};

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		string indicator;
		std::vector<string> countries;
		sudan::FilterResult year_filter;

		explicit BindData(const string &indicator, const std::vector<string> &countries)
		    : indicator(indicator), countries(std::move(countries)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 1);
		const string indicator = StringValue::Get(input.inputs[0]);

		if (indicator.empty()) {
			throw InvalidInputException("SUDAN: The indicator parameter cannot be empty.");
		}

		// Extract countries from named parameters, default to Sudan only
		std::vector<string> countries;
		auto options_param = input.named_parameters.find("countries");
		if (options_param != input.named_parameters.end()) {
			auto &items = options_param->second;
			if (!items.IsNull() && items.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
				for (const auto &item : ListValue::GetChildren(items)) {
					auto code = item.GetValue<string>();
					countries.push_back(sudan::NormalizeCountryCode(code));
				}
			}
		}
		if (countries.empty()) {
			countries.push_back("SDN");
		}

		names.emplace_back("indicator_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("indicator_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("country");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("country_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("year");
		return_types.push_back(LogicalType::INTEGER);
		names.emplace_back("value");
		return_types.push_back(LogicalType::DOUBLE);

		return make_uniq_base<FunctionData, BindData>(indicator, countries);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		std::vector<DataRow> rows;
		idx_t current_row;

		explicit State() : current_row(0) {
		}
	};

	//! Fetch all pages of World Bank data for one country and indicator
	static void FetchWorldBankData(const HttpSettings &settings, const string &indicator,
	                               const string &country_iso3, const sudan::FilterResult &year_filter,
	                               std::vector<DataRow> &rows) {

		// Build base URL: https://api.worldbank.org/v2/country/{iso3}/indicator/{indicator}
		string base_url = "https://api.worldbank.org/v2/country/" + country_iso3 + "/indicator/" + indicator;

		int page = 1;
		int total_pages = 1;

		while (page <= total_pages) {
			string url = base_url + "?format=json&per_page=1000&page=" + std::to_string(page);

			// Add year filter
			string year_param = sudan::EncodeWorldBankYearFilter(year_filter);
			if (!year_param.empty()) {
				url += "&" + year_param;
			}

			// Check cache first
			auto &cache = sudan::ResponseCache::Instance();
			string body = cache.Get(url);

			if (body.empty()) {
				auto response = HttpClient::Get(settings, url);
				if (response.status_code != 200 || !response.error.empty()) {
					break;
				}
				body = response.body;
				cache.Put(url, body);
			}

			// Parse JSON response
			auto json_data = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
			if (!json_data) {
				break;
			}

			auto root_val = yyjson_doc_get_root(json_data);

			// World Bank V2 API returns an array: [metadata, data]
			if (!yyjson_is_arr(root_val) || yyjson_arr_size(root_val) < 2) {
				yyjson_doc_free(json_data);
				break;
			}

			// Parse pagination metadata
			auto meta = yyjson_arr_get(root_val, 0);
			if (yyjson_is_obj(meta)) {
				auto pages_val = yyjson_obj_get(meta, "pages");
				if (yyjson_is_int(pages_val)) {
					total_pages = yyjson_get_int(pages_val);
				}
			}

			// Parse data array
			auto data_arr = yyjson_arr_get(root_val, 1);
			if (yyjson_is_arr(data_arr)) {
				auto arr_len = yyjson_arr_size(data_arr);

				for (size_t i = 0; i < arr_len; i++) {
					auto elem = yyjson_arr_get(data_arr, i);

					DataRow row;

					// indicator
					auto ind_obj = yyjson_obj_get(elem, "indicator");
					if (yyjson_is_obj(ind_obj)) {
						auto id_val = yyjson_obj_get(ind_obj, "id");
						auto name_val = yyjson_obj_get(ind_obj, "value");
						if (yyjson_is_str(id_val)) {
							row.indicator_id = yyjson_get_str(id_val);
						}
						if (yyjson_is_str(name_val)) {
							row.indicator_name = yyjson_get_str(name_val);
						}
					}

					// country
					auto country_obj = yyjson_obj_get(elem, "country");
					if (yyjson_is_obj(country_obj)) {
						auto id_val = yyjson_obj_get(country_obj, "id");
						auto name_val = yyjson_obj_get(country_obj, "value");
						if (yyjson_is_str(id_val)) {
							row.country_id = yyjson_get_str(id_val);
						}
						if (yyjson_is_str(name_val)) {
							row.country_name = yyjson_get_str(name_val);
						}
					}

					// date (year)
					auto date_val = yyjson_obj_get(elem, "date");
					if (yyjson_is_str(date_val)) {
						try {
							row.year = std::stoi(yyjson_get_str(date_val));
						} catch (...) {
							row.year = 0;
						}
					}

					// value
					auto value_val = yyjson_obj_get(elem, "value");
					if (yyjson_is_real(value_val)) {
						row.value = yyjson_get_real(value_val);
						row.has_value = true;
					} else if (yyjson_is_int(value_val)) {
						row.value = static_cast<double>(yyjson_get_int(value_val));
						row.has_value = true;
					} else {
						row.value = 0.0;
						row.has_value = false;
					}

					rows.push_back(row);
				}
			}

			yyjson_doc_free(json_data);
			page++;
		}
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &state = global_state->Cast<State>();

		HttpSettings settings = HttpClient::ExtractHttpSettings(context, "https://api.worldbank.org");
		settings.timeout = 90;

		for (const auto &country : bind_data.countries) {
			FetchWorldBankData(settings, bind_data.indicator, country, bind_data.year_filter, state.rows);
		}

		return global_state;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &state = input.global_state->Cast<State>();

		const auto output_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.current_row);

		if (output_size == 0) {
			output.SetCardinality(0);
			return;
		}

		for (idx_t row_idx = 0; row_idx < output_size; row_idx++) {
			const auto &row = state.rows[state.current_row + row_idx];
			output.data[0].SetValue(row_idx, row.indicator_id);
			output.data[1].SetValue(row_idx, row.indicator_name);
			output.data[2].SetValue(row_idx, row.country_id);
			output.data[3].SetValue(row_idx, row.country_name);
			output.data[4].SetValue(row_idx, Value::INTEGER(row.year));
			if (row.has_value) {
				output.data[5].SetValue(row_idx, Value::DOUBLE(row.value));
			} else {
				output.data[5].SetValue(row_idx, Value());
			}
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Reads World Bank indicator data for Sudan and neighboring countries.
		The indicator parameter specifies the World Bank indicator code (e.g., 'SP.POP.TOTL' for total population).
		By default, data is fetched for Sudan only. Use the 'countries' parameter to include neighboring countries.
	)";

	static constexpr auto EXAMPLE = R"(
		-- Sudan population over time
		SELECT * FROM SUDAN_WorldBank('SP.POP.TOTL');

		-- Compare Sudan with Egypt and South Sudan
		SELECT * FROM SUDAN_WorldBank('SP.POP.TOTL', countries := ['SDN', 'EGY', 'SSD'])
		WHERE year >= 2010 AND year <= 2023;

		+---------------+------------------+---------+--------------+------+-----------+
		| indicator_id  | indicator_name   | country | country_name | year | value     |
		+---------------+------------------+---------+--------------+------+-----------+
		| SP.POP.TOTL   | Population, total| SD      | Sudan        | 2023 | 48109006  |
		+---------------+------------------+---------+--------------+------+-----------+
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_WorldBank", {LogicalType::VARCHAR}, Execute, Bind, Init);
		func.named_parameters["countries"] = LogicalType::LIST(LogicalType::VARCHAR);

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

//======================================================================================================================
// Register World Bank Functions
//======================================================================================================================

void WorldBankFunctions::Register(ExtensionLoader &loader) {
	SudanWorldBank::Register(loader);
}

} // namespace duckdb
