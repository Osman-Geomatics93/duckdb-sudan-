#include "unhcr_functions.hpp"
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
#include "sudan/cache.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// SUDAN_UNHCR
//======================================================================================================================

struct SudanUNHCR {

	struct DataRow {
		int32_t year;
		string population_type;
		string country_origin;
		string country_origin_name;
		string country_asylum;
		string country_asylum_name;
		int64_t value;
		bool has_value;
	};

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		string population_type;
		std::vector<string> countries;

		explicit BindData(const string &population_type, const std::vector<string> &countries)
		    : population_type(population_type), countries(std::move(countries)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 1);
		const string population_type = StringValue::Get(input.inputs[0]);

		if (population_type.empty()) {
			throw InvalidInputException(
			    "SUDAN: The population_type parameter cannot be empty for SUDAN_UNHCR(). "
			    "Valid types: 'refugees', 'idps', 'asylum_seekers', 'returned_refugees', 'stateless'.");
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

		names.emplace_back("year");
		return_types.push_back(LogicalType::INTEGER);
		names.emplace_back("population_type");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("country_origin");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("country_origin_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("country_asylum");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("country_asylum_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("value");
		return_types.push_back(LogicalType::BIGINT);

		return make_uniq_base<FunctionData, BindData>(population_type, countries);
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

	//! Map population type to UNHCR API endpoint path
	static string GetUNHCRPopulationType(const string &type) {
		string type_lower = StringUtil::Lower(type);
		if (type_lower == "refugees" || type_lower == "ref") {
			return "refugees";
		}
		if (type_lower == "idps" || type_lower == "idp") {
			return "idps";
		}
		if (type_lower == "asylum_seekers" || type_lower == "asylum") {
			return "asylum-seekers";
		}
		if (type_lower == "returned_refugees" || type_lower == "returned") {
			return "returned-refugees";
		}
		if (type_lower == "stateless") {
			return "stateless";
		}
		return type_lower;
	}

	static void FetchUNHCRData(const HttpSettings &settings, const string &population_type,
	                           const string &country_iso3, std::vector<DataRow> &rows) {

		string pop_type = GetUNHCRPopulationType(population_type);

		// UNHCR Population Statistics API
		// Try both as origin and asylum country
		for (const auto &param_name : {"coo", "coa"}) {
			string url = "https://api.unhcr.org/population/v1/" + pop_type +
			             "/?limit=10000&" + string(param_name) + "=" + country_iso3;

			auto &cache = sudan::ResponseCache::Instance();
			string body = cache.Get(url);

			if (body.empty()) {
				auto response = HttpClient::Get(settings, url);
				if (response.status_code != 200 || !response.error.empty()) {
					continue;
				}
				body = response.body;
				cache.Put(url, body);
			}

			auto json_data = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
			if (!json_data) {
				continue;
			}

			auto root_val = yyjson_doc_get_root(json_data);
			auto items_arr = yyjson_obj_get(root_val, "items");
			if (!yyjson_is_arr(items_arr)) {
				yyjson_doc_free(json_data);
				continue;
			}

			auto arr_len = yyjson_arr_size(items_arr);

			for (size_t i = 0; i < arr_len; i++) {
				auto elem = yyjson_arr_get(items_arr, i);

				DataRow row;
				row.population_type = population_type;
				row.has_value = false;

				auto year_val = yyjson_obj_get(elem, "year");
				if (yyjson_is_int(year_val)) {
					row.year = yyjson_get_int(year_val);
				}

				auto coo_val = yyjson_obj_get(elem, "coo");
				if (yyjson_is_str(coo_val)) {
					row.country_origin = yyjson_get_str(coo_val);
				}
				auto coo_name_val = yyjson_obj_get(elem, "coo_name");
				if (yyjson_is_str(coo_name_val)) {
					row.country_origin_name = yyjson_get_str(coo_name_val);
				}

				auto coa_val = yyjson_obj_get(elem, "coa");
				if (yyjson_is_str(coa_val)) {
					row.country_asylum = yyjson_get_str(coa_val);
				}
				auto coa_name_val = yyjson_obj_get(elem, "coa_name");
				if (yyjson_is_str(coa_name_val)) {
					row.country_asylum_name = yyjson_get_str(coa_name_val);
				}

				// Different fields depending on population type
				auto total_val = yyjson_obj_get(elem, "refugees");
				if (!total_val) {
					total_val = yyjson_obj_get(elem, "idps");
				}
				if (!total_val) {
					total_val = yyjson_obj_get(elem, "asylum_seekers");
				}
				if (!total_val) {
					total_val = yyjson_obj_get(elem, "returned_refugees");
				}
				if (!total_val) {
					total_val = yyjson_obj_get(elem, "stateless");
				}
				if (!total_val) {
					total_val = yyjson_obj_get(elem, "total");
				}

				if (yyjson_is_int(total_val)) {
					row.value = yyjson_get_int(total_val);
					row.has_value = true;
				} else if (yyjson_is_real(total_val)) {
					row.value = static_cast<int64_t>(yyjson_get_real(total_val));
					row.has_value = true;
				}

				rows.push_back(row);
			}

			yyjson_doc_free(json_data);
		}
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &state = global_state->Cast<State>();

		HttpSettings settings = HttpClient::ExtractHttpSettings(context, "https://api.unhcr.org");
		settings.timeout = 90;

		for (const auto &country : bind_data.countries) {
			FetchUNHCRData(settings, bind_data.population_type, country, state.rows);
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
			output.data[0].SetValue(row_idx, Value::INTEGER(row.year));
			output.data[1].SetValue(row_idx, row.population_type);
			output.data[2].SetValue(row_idx, row.country_origin);
			output.data[3].SetValue(row_idx, row.country_origin_name);
			output.data[4].SetValue(row_idx, row.country_asylum);
			output.data[5].SetValue(row_idx, row.country_asylum_name);
			if (row.has_value) {
				output.data[6].SetValue(row_idx, Value::BIGINT(row.value));
			} else {
				output.data[6].SetValue(row_idx, Value());
			}
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Reads UNHCR displacement and population data for Sudan and neighboring countries.
		The population_type parameter specifies the type of population data:
		'refugees', 'idps', 'asylum_seekers', 'returned_refugees', 'stateless'.
	)";

	static constexpr auto EXAMPLE = R"(
		-- UNHCR displacement data for Sudan
		SELECT * FROM SUDAN_UNHCR('idps');

		-- Compare Sudan and South Sudan refugee data
		SELECT * FROM SUDAN_UNHCR('refugees', countries := ['SDN', 'SSD']);
	)";

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_UNHCR", {LogicalType::VARCHAR}, Execute, Bind, Init);
		func.named_parameters["countries"] = LogicalType::LIST(LogicalType::VARCHAR);

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

//======================================================================================================================
// Register UNHCR Functions
//======================================================================================================================

void UNHCRFunctions::Register(ExtensionLoader &loader) {
	SudanUNHCR::Register(loader);
}

} // namespace duckdb
