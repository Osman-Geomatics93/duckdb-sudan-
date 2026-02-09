#include "ilo_functions.hpp"
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
// SUDAN_ILO
//======================================================================================================================

struct SudanILO {

	struct DataRow {
		string indicator;
		string country;
		string sex;
		string classif1;
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

		explicit BindData(const string &indicator, const std::vector<string> &countries)
		    : indicator(indicator), countries(std::move(countries)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 1);
		const string indicator = StringValue::Get(input.inputs[0]);

		if (indicator.empty()) {
			throw InvalidInputException("SUDAN: The indicator parameter cannot be empty for SUDAN_ILO().");
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

		names.emplace_back("indicator");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("country");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("sex");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("classif1");
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

	static void FetchILOData(const HttpSettings &settings, const string &indicator, const string &country_iso3,
	                         std::vector<DataRow> &rows) {

		// ILOSTAT SDMX REST API for data
		// The ILOSTAT API uses SDMX-JSON format
		string url = "https://www.ilo.org/sdmx/rest/data/ILO,DF_" + indicator + "/" +
		             country_iso3 + "..?format=jsondata&detail=dataonly";

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

		// SDMX-JSON format: dataSets[0].observations or dataSets[0].series
		auto datasets_arr = yyjson_obj_get(root_val, "dataSets");
		if (!yyjson_is_arr(datasets_arr) || yyjson_arr_size(datasets_arr) == 0) {
			yyjson_doc_free(json_data);
			return;
		}

		auto dataset = yyjson_arr_get(datasets_arr, 0);

		// Try flat observations first (simpler format)
		auto observations = yyjson_obj_get(dataset, "observations");
		if (yyjson_is_obj(observations)) {
			// Get dimension values from structure
			auto structure = yyjson_obj_get(root_val, "structure");
			auto dimensions = yyjson_obj_get(structure, "dimensions");
			auto observation_dims = yyjson_obj_get(dimensions, "observation");

			// Extract time period values
			std::vector<string> time_values;
			if (yyjson_is_arr(observation_dims)) {
				auto obs_dim_len = yyjson_arr_size(observation_dims);
				for (size_t d = 0; d < obs_dim_len; d++) {
					auto dim = yyjson_arr_get(observation_dims, d);
					auto dim_id = yyjson_obj_get(dim, "id");
					if (yyjson_is_str(dim_id) && string(yyjson_get_str(dim_id)) == "TIME_PERIOD") {
						auto values = yyjson_obj_get(dim, "values");
						if (yyjson_is_arr(values)) {
							auto val_len = yyjson_arr_size(values);
							for (size_t v = 0; v < val_len; v++) {
								auto val_obj = yyjson_arr_get(values, v);
								auto val_id = yyjson_obj_get(val_obj, "id");
								if (yyjson_is_str(val_id)) {
									time_values.push_back(yyjson_get_str(val_id));
								}
							}
						}
					}
				}
			}

			// Parse observations
			yyjson_val *key, *val;
			yyjson_obj_iter iter;
			yyjson_obj_iter_init(observations, &iter);
			while ((key = yyjson_obj_iter_next(&iter))) {
				val = yyjson_obj_iter_get_val(key);

				DataRow row;
				row.indicator = indicator;
				row.country = country_iso3;
				row.has_value = false;

				// The key is colon-separated indices like "0:0:0:5"
				string obs_key = yyjson_get_str(key);

				// Extract time period from the last index
				auto last_colon = obs_key.rfind(':');
				if (last_colon != string::npos) {
					try {
						size_t time_idx = std::stoul(obs_key.substr(last_colon + 1));
						if (time_idx < time_values.size()) {
							try {
								row.year = std::stoi(time_values[time_idx]);
							} catch (...) {
								row.year = 0;
							}
						}
					} catch (...) {
					}
				}

				// Value is in array format: [value]
				if (yyjson_is_arr(val) && yyjson_arr_size(val) > 0) {
					auto first_val = yyjson_arr_get(val, 0);
					if (yyjson_is_real(first_val)) {
						row.value = yyjson_get_real(first_val);
						row.has_value = true;
					} else if (yyjson_is_int(first_val)) {
						row.value = static_cast<double>(yyjson_get_int(first_val));
						row.has_value = true;
					}
				}

				if (row.has_value) {
					rows.push_back(row);
				}
			}
		}

		// Try series format
		auto series = yyjson_obj_get(dataset, "series");
		if (yyjson_is_obj(series) && !yyjson_is_obj(observations)) {
			// Similar parsing for series format
			yyjson_val *series_key, *series_val;
			yyjson_obj_iter series_iter;
			yyjson_obj_iter_init(series, &series_iter);
			while ((series_key = yyjson_obj_iter_next(&series_iter))) {
				series_val = yyjson_obj_iter_get_val(series_key);

				auto obs = yyjson_obj_get(series_val, "observations");
				if (!yyjson_is_obj(obs)) {
					continue;
				}

				yyjson_val *obs_key, *obs_val;
				yyjson_obj_iter obs_iter;
				yyjson_obj_iter_init(obs, &obs_iter);
				while ((obs_key = yyjson_obj_iter_next(&obs_iter))) {
					obs_val = yyjson_obj_iter_get_val(obs_key);

					DataRow row;
					row.indicator = indicator;
					row.country = country_iso3;
					row.has_value = false;

					if (yyjson_is_arr(obs_val) && yyjson_arr_size(obs_val) > 0) {
						auto first_val = yyjson_arr_get(obs_val, 0);
						if (yyjson_is_real(first_val)) {
							row.value = yyjson_get_real(first_val);
							row.has_value = true;
						} else if (yyjson_is_int(first_val)) {
							row.value = static_cast<double>(yyjson_get_int(first_val));
							row.has_value = true;
						}
					}

					if (row.has_value) {
						rows.push_back(row);
					}
				}
			}
		}

		yyjson_doc_free(json_data);
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &state = global_state->Cast<State>();

		HttpSettings settings = HttpClient::ExtractHttpSettings(context, "https://www.ilo.org");
		settings.timeout = 90;

		for (const auto &country : bind_data.countries) {
			FetchILOData(settings, bind_data.indicator, country, state.rows);
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
			output.data[0].SetValue(row_idx, row.indicator);
			output.data[1].SetValue(row_idx, row.country);
			output.data[2].SetValue(row_idx, row.sex.empty() ? Value() : Value(row.sex));
			output.data[3].SetValue(row_idx, row.classif1.empty() ? Value() : Value(row.classif1));
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
		Reads ILO (International Labour Organization) statistics for Sudan and neighboring countries.
	)";

	static constexpr auto EXAMPLE = R"(
		-- Unemployment rate
		SELECT * FROM SUDAN_ILO('UNE_DEAP_SEX_AGE_RT');

		-- Compare with neighbors
		SELECT * FROM SUDAN_ILO('UNE_DEAP_SEX_AGE_RT', countries := ['SDN', 'EGY']);
	)";

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_ILO", {LogicalType::VARCHAR}, Execute, Bind, Init);
		func.named_parameters["countries"] = LogicalType::LIST(LogicalType::VARCHAR);

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

//======================================================================================================================
// Register ILO Functions
//======================================================================================================================

void ILOFunctions::Register(ExtensionLoader &loader) {
	SudanILO::Register(loader);
}

} // namespace duckdb
