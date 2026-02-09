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
		// Base URL: sdmx.ilo.org/rest (changed from www.ilo.org/sdmx/rest in 2024)
		// Key format: REF_AREA.FREQ.remaining_dims (country first, then A for Annual)
		// Dataflow IDs already have DF_ prefix in the catalog
		string dataflow = indicator;
		if (dataflow.substr(0, 3) != "DF_") {
			dataflow = "DF_" + dataflow;
		}
		// The number of dimensions varies per indicator, so try multiple key lengths.
		// SDMX wildcards empty positions with dots. Most ILO indicators have 3-5 dimensions
		// after REF_AREA and FREQ.
		string base = "https://sdmx.ilo.org/rest/data/ILO," + dataflow + "/" +
		              country_iso3 + ".A";
		string suffix = "?format=jsondata&detail=dataonly&lastNObservations=20";

		// Try keys with 1 to 5 wildcarded dimensions after FREQ
		static const char *key_suffixes[] = {".", "..", "...", "....", "....."};

		auto &cache = sudan::ResponseCache::Instance();
		string body;

		for (const auto &ks : key_suffixes) {
			string url = base + string(ks) + suffix;
			body = cache.Get(url);
			if (!body.empty()) {
				break;
			}
			auto response = HttpClient::Get(settings, url);
			if (response.status_code == 200 && response.error.empty() && !response.body.empty()) {
				body = response.body;
				cache.Put(url, body);
				break;
			}
		}

		if (body.empty()) {
			return; // All key formats failed or server unavailable
		}

		auto json_data = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
		if (!json_data) {
			return;
		}

		auto root_val = yyjson_doc_get_root(json_data);

		// SDMX-JSON 2.0 uses "data" > "dataSets", while 1.0 uses "dataSets" at root
		auto datasets_arr = yyjson_obj_get(root_val, "dataSets");
		if (!yyjson_is_arr(datasets_arr) || yyjson_arr_size(datasets_arr) == 0) {
			// Try SDMX-JSON 2.0 format: root > data > dataSets
			auto data_obj = yyjson_obj_get(root_val, "data");
			if (yyjson_is_obj(data_obj)) {
				datasets_arr = yyjson_obj_get(data_obj, "dataSets");
			}
		}
		if (!yyjson_is_arr(datasets_arr) || yyjson_arr_size(datasets_arr) == 0) {
			yyjson_doc_free(json_data);
			return;
		}

		auto dataset = yyjson_arr_get(datasets_arr, 0);

		// Get structure — in SDMX-JSON 2.0 it's under data.structures[0], in 1.0 it's at root
		yyjson_val *structure = yyjson_obj_get(root_val, "structure");
		if (!structure) {
			auto data_obj = yyjson_obj_get(root_val, "data");
			if (yyjson_is_obj(data_obj)) {
				auto structures_arr = yyjson_obj_get(data_obj, "structures");
				if (yyjson_is_arr(structures_arr) && yyjson_arr_size(structures_arr) > 0) {
					structure = yyjson_arr_get(structures_arr, 0);
				}
			}
		}

		// Helper: extract string values from a dimension's values array
		auto ExtractDimValues = [](yyjson_val *dim) -> std::vector<string> {
			std::vector<string> result;
			auto values = yyjson_obj_get(dim, "values");
			if (yyjson_is_arr(values)) {
				auto len = yyjson_arr_size(values);
				for (size_t i = 0; i < len; i++) {
					auto val_obj = yyjson_arr_get(values, i);
					auto val_id = yyjson_obj_get(val_obj, "id");
					if (yyjson_is_str(val_id)) {
						result.push_back(yyjson_get_str(val_id));
					} else {
						auto val_name = yyjson_obj_get(val_obj, "name");
						if (yyjson_is_str(val_name)) {
							result.push_back(yyjson_get_str(val_name));
						} else {
							result.push_back("");
						}
					}
				}
			}
			return result;
		};

		// Helper: parse colon-separated key into indices
		auto ParseKeyIndices = [](const string &key) -> std::vector<size_t> {
			std::vector<size_t> indices;
			size_t start = 0;
			while (start <= key.size()) {
				auto colon = key.find(':', start);
				if (colon == string::npos) {
					colon = key.size();
				}
				try {
					indices.push_back(std::stoul(key.substr(start, colon - start)));
				} catch (...) {
					indices.push_back(0);
				}
				start = colon + 1;
			}
			return indices;
		};

		// Helper: extract value from observation array [value, ...]
		auto ExtractObsValue = [](yyjson_val *obs_val, double &value) -> bool {
			if (yyjson_is_arr(obs_val) && yyjson_arr_size(obs_val) > 0) {
				auto first_val = yyjson_arr_get(obs_val, 0);
				if (yyjson_is_real(first_val)) {
					value = yyjson_get_real(first_val);
					return true;
				} else if (yyjson_is_int(first_val)) {
					value = static_cast<double>(yyjson_get_int(first_val));
					return true;
				}
			}
			return false;
		};

		// Build dimension lookup tables from structure
		// Series dimensions: REF_AREA, FREQ, SEX, AGE, MEASURE, etc.
		// Observation dimensions: TIME_PERIOD
		struct DimInfo {
			string id;
			std::vector<string> values;
		};
		std::vector<DimInfo> series_dims;
		std::vector<DimInfo> obs_dims;

		if (structure) {
			auto dimensions = yyjson_obj_get(structure, "dimensions");

			// Series dimensions
			auto series_dim_arr = yyjson_obj_get(dimensions, "series");
			if (yyjson_is_arr(series_dim_arr)) {
				auto len = yyjson_arr_size(series_dim_arr);
				for (size_t i = 0; i < len; i++) {
					auto dim = yyjson_arr_get(series_dim_arr, i);
					DimInfo info;
					auto dim_id = yyjson_obj_get(dim, "id");
					info.id = yyjson_is_str(dim_id) ? yyjson_get_str(dim_id) : "";
					info.values = ExtractDimValues(dim);
					series_dims.push_back(std::move(info));
				}
			}

			// Observation dimensions
			auto obs_dim_arr = yyjson_obj_get(dimensions, "observation");
			if (yyjson_is_arr(obs_dim_arr)) {
				auto len = yyjson_arr_size(obs_dim_arr);
				for (size_t i = 0; i < len; i++) {
					auto dim = yyjson_arr_get(obs_dim_arr, i);
					DimInfo info;
					auto dim_id = yyjson_obj_get(dim, "id");
					info.id = yyjson_is_str(dim_id) ? yyjson_get_str(dim_id) : "";
					info.values = ExtractDimValues(dim);
					obs_dims.push_back(std::move(info));
				}
			}
		}

		// Helper: look up a dimension value by ID and index
		auto LookupDimValue = [](const std::vector<DimInfo> &dims, const string &dim_id,
		                         const std::vector<size_t> &indices) -> string {
			for (size_t i = 0; i < dims.size() && i < indices.size(); i++) {
				if (dims[i].id == dim_id && indices[i] < dims[i].values.size()) {
					return dims[i].values[indices[i]];
				}
			}
			return "";
		};

		// Parse series format (SDMX-JSON 2.0 — used by ILO)
		auto series = yyjson_obj_get(dataset, "series");
		if (yyjson_is_obj(series)) {
			yyjson_val *series_key, *series_val;
			yyjson_obj_iter series_iter;
			yyjson_obj_iter_init(series, &series_iter);
			while ((series_key = yyjson_obj_iter_next(&series_iter))) {
				series_val = yyjson_obj_iter_get_val(series_key);

				string sk = yyjson_get_str(series_key);
				auto series_indices = ParseKeyIndices(sk);

				// Extract dimension values from series key
				string sex = LookupDimValue(series_dims, "SEX", series_indices);
				string classif1 = LookupDimValue(series_dims, "AGE", series_indices);
				if (classif1.empty()) {
					classif1 = LookupDimValue(series_dims, "CLASSIF1", series_indices);
				}

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
					row.sex = sex;
					row.classif1 = classif1;
					row.has_value = false;

					// Observation key maps to observation dimensions (typically TIME_PERIOD)
					string ok = yyjson_get_str(obs_key);
					auto obs_indices = ParseKeyIndices(ok);
					string time_str = LookupDimValue(obs_dims, "TIME_PERIOD", obs_indices);
					try {
						row.year = std::stoi(time_str);
					} catch (...) {
						row.year = 0;
					}

					if (ExtractObsValue(obs_val, row.value)) {
						row.has_value = true;
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

		HttpSettings settings = HttpClient::ExtractHttpSettings(context, "https://sdmx.ilo.org");
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
