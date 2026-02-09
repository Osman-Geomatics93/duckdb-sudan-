#include "who_functions.hpp"
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
// SUDAN_WHO
//======================================================================================================================

struct SudanWHO {

	struct DataRow {
		string indicator_code;
		string indicator_name;
		string country;
		int32_t year;
		string sex;
		double value;
		bool has_value;
		string region;
	};

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		string indicator;
		std::vector<string> countries;

		explicit BindData(const string &indicator, const std::vector<string> &countries)
		    : indicator(indicator), countries(std::move(countries)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 1);
		const string indicator = StringValue::Get(input.inputs[0]);

		if (indicator.empty()) {
			throw InvalidInputException("SUDAN: The indicator parameter cannot be empty for SUDAN_WHO().");
		}

		std::vector<string> countries;
		auto options_param = input.named_parameters.find("countries");
		if (options_param != input.named_parameters.end()) {
			auto &items = options_param->second;
			if (!items.IsNull() && items.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
				for (const auto &item : ListValue::GetChildren(items)) {
					countries.push_back(sudan::NormalizeCountryCode(item.GetValue<string>()));
				}
			}
		}
		if (countries.empty()) {
			countries.push_back("SDN");
		}

		names.emplace_back("indicator_code");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("indicator_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("country");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("year");
		return_types.push_back(LogicalType::INTEGER);
		names.emplace_back("sex");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("value");
		return_types.push_back(LogicalType::DOUBLE);
		names.emplace_back("region");
		return_types.push_back(LogicalType::VARCHAR);

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

	static void FetchWHOData(const HttpSettings &settings, const string &indicator, const string &country_iso3,
	                         std::vector<DataRow> &rows) {

		// WHO GHO OData API: https://ghoapi.azureedge.net/api/{indicator}?$filter=SpatialDim eq '{country}'
		string url = "https://ghoapi.azureedge.net/api/" + indicator +
		             "?$filter=SpatialDim eq '" + country_iso3 + "'";

		auto &cache = sudan::ResponseCache::Instance();
		string body = cache.Get(url);

		if (body.empty()) {
			auto response = HttpClient::Get(settings, url);
			if (response.status_code != 200 || !response.error.empty()) {
				return;
			}
			body = response.body;
			cache.Put(url, body);
		}

		auto json_data = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
		if (!json_data) {
			return;
		}

		auto root_val = yyjson_doc_get_root(json_data);
		auto value_arr = yyjson_obj_get(root_val, "value");
		if (!yyjson_is_arr(value_arr)) {
			yyjson_doc_free(json_data);
			return;
		}

		auto arr_len = yyjson_arr_size(value_arr);

		for (size_t i = 0; i < arr_len; i++) {
			auto elem = yyjson_arr_get(value_arr, i);

			DataRow row;
			row.indicator_code = indicator;
			row.country = country_iso3;
			row.has_value = false;
			row.year = 0;

			// IndicatorCode
			auto code_val = yyjson_obj_get(elem, "IndicatorCode");
			if (yyjson_is_str(code_val)) {
				row.indicator_code = yyjson_get_str(code_val);
			}

			// TimeDim (year)
			auto time_val = yyjson_obj_get(elem, "TimeDim");
			if (yyjson_is_int(time_val)) {
				row.year = yyjson_get_int(time_val);
			} else if (yyjson_is_str(time_val)) {
				try {
					row.year = std::stoi(yyjson_get_str(time_val));
				} catch (...) {
					row.year = 0;
				}
			}

			// SpatialDim
			auto spatial_val = yyjson_obj_get(elem, "SpatialDim");
			if (yyjson_is_str(spatial_val)) {
				row.country = yyjson_get_str(spatial_val);
			}

			// Dim1 (sex)
			auto dim1_val = yyjson_obj_get(elem, "Dim1");
			if (yyjson_is_str(dim1_val)) {
				row.sex = yyjson_get_str(dim1_val);
			}

			// NumericValue
			auto num_val = yyjson_obj_get(elem, "NumericValue");
			if (yyjson_is_real(num_val)) {
				row.value = yyjson_get_real(num_val);
				row.has_value = true;
			} else if (yyjson_is_int(num_val)) {
				row.value = static_cast<double>(yyjson_get_int(num_val));
				row.has_value = true;
			}

			// ParentLocation (region)
			auto parent_val = yyjson_obj_get(elem, "ParentLocation");
			if (yyjson_is_str(parent_val)) {
				row.region = yyjson_get_str(parent_val);
			}

			// Get indicator name from first element
			if (rows.empty()) {
				// We'll set indicator_name from the API if available
				// WHO GHO doesn't include indicator name in data responses
			}

			rows.push_back(row);
		}

		yyjson_doc_free(json_data);
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &state = global_state->Cast<State>();

		HttpSettings settings = HttpClient::ExtractHttpSettings(context, "https://ghoapi.azureedge.net");
		settings.timeout = 90;

		for (const auto &country : bind_data.countries) {
			FetchWHOData(settings, bind_data.indicator, country, state.rows);
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
			output.data[0].SetValue(row_idx, row.indicator_code);
			output.data[1].SetValue(row_idx, row.indicator_name.empty() ? Value() : Value(row.indicator_name));
			output.data[2].SetValue(row_idx, row.country);
			output.data[3].SetValue(row_idx, Value::INTEGER(row.year));
			output.data[4].SetValue(row_idx, row.sex.empty() ? Value() : Value(row.sex));
			if (row.has_value) {
				output.data[5].SetValue(row_idx, Value::DOUBLE(row.value));
			} else {
				output.data[5].SetValue(row_idx, Value());
			}
			output.data[6].SetValue(row_idx, row.region.empty() ? Value() : Value(row.region));
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Reads WHO Global Health Observatory (GHO) indicator data for Sudan and neighboring countries.
	)";

	static constexpr auto EXAMPLE = R"(
		-- Life expectancy at birth
		SELECT * FROM SUDAN_WHO('WHOSIS_000001') WHERE year >= 2015;

		-- Compare Sudan and South Sudan
		SELECT * FROM SUDAN_WHO('WHOSIS_000001', countries := ['SDN', 'SSD']);
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_WHO", {LogicalType::VARCHAR}, Execute, Bind, Init);
		func.named_parameters["countries"] = LogicalType::LIST(LogicalType::VARCHAR);

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

//======================================================================================================================
// SUDAN_WHO_Indicators
//======================================================================================================================

struct SudanWHOIndicators {

	struct IndicatorInfo {
		string code;
		string name;
		string language;
	};

	struct BindData final : TableFunctionData {
		string search;
		explicit BindData(const string &search) : search(search) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		string search;
		auto options_param = input.named_parameters.find("search");
		if (options_param != input.named_parameters.end()) {
			auto &item = options_param->second;
			if (!item.IsNull() && item.type() == LogicalType::VARCHAR) {
				search = item.GetValue<string>();
			}
		}

		names.emplace_back("indicator_code");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("indicator_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("language");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(search);
	}

	struct State final : GlobalTableFunctionState {
		std::vector<IndicatorInfo> rows;
		idx_t current_row;
		explicit State() : current_row(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &state = global_state->Cast<State>();

		string url = "https://ghoapi.azureedge.net/api/Indicator";

		auto &cache = sudan::ResponseCache::Instance();
		string body = cache.Get(url);

		if (body.empty()) {
			auto response = HttpClient::Get(context, url);
			if (response.status_code != 200 || !response.error.empty()) {
				return global_state;
			}
			body = response.body;
			cache.Put(url, body);
		}

		auto json_data = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
		if (!json_data) {
			return global_state;
		}

		auto root_val = yyjson_doc_get_root(json_data);
		auto value_arr = yyjson_obj_get(root_val, "value");
		if (!yyjson_is_arr(value_arr)) {
			yyjson_doc_free(json_data);
			return global_state;
		}

		string search_lower = StringUtil::Lower(bind_data.search);
		auto arr_len = yyjson_arr_size(value_arr);

		for (size_t i = 0; i < arr_len; i++) {
			auto elem = yyjson_arr_get(value_arr, i);

			IndicatorInfo info;
			auto code_val = yyjson_obj_get(elem, "IndicatorCode");
			auto name_val = yyjson_obj_get(elem, "IndicatorName");
			auto lang_val = yyjson_obj_get(elem, "Language");

			if (yyjson_is_str(code_val)) {
				info.code = yyjson_get_str(code_val);
			}
			if (yyjson_is_str(name_val)) {
				info.name = yyjson_get_str(name_val);
			}
			if (yyjson_is_str(lang_val)) {
				info.language = yyjson_get_str(lang_val);
			}

			if (!search_lower.empty()) {
				string name_lower = StringUtil::Lower(info.name);
				string code_lower = StringUtil::Lower(info.code);
				if (name_lower.find(search_lower) == string::npos &&
				    code_lower.find(search_lower) == string::npos) {
					continue;
				}
			}

			state.rows.push_back(info);
		}

		yyjson_doc_free(json_data);
		return global_state;
	}

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &state = input.global_state->Cast<State>();

		const auto output_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.current_row);

		if (output_size == 0) {
			output.SetCardinality(0);
			return;
		}

		for (idx_t row_idx = 0; row_idx < output_size; row_idx++) {
			const auto &info = state.rows[state.current_row + row_idx];
			output.data[0].SetValue(row_idx, info.code);
			output.data[1].SetValue(row_idx, info.name);
			output.data[2].SetValue(row_idx, info.language.empty() ? Value() : Value(info.language));
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	static constexpr auto DESCRIPTION = R"(
		Lists WHO Global Health Observatory indicator codes. Optionally filter by search term.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT * FROM SUDAN_WHO_Indicators(search := 'mortality') LIMIT 5;
	)";

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_WHO_Indicators", {}, Execute, Bind, Init);
		func.named_parameters["search"] = LogicalType::VARCHAR;

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

//======================================================================================================================
// Register WHO Functions
//======================================================================================================================

void WHOFunctions::Register(ExtensionLoader &loader) {
	SudanWHO::Register(loader);
	SudanWHOIndicators::Register(loader);
}

} // namespace duckdb
