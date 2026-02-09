#include "fao_functions.hpp"
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
// SUDAN_FAO
//======================================================================================================================

struct SudanFAO {

	struct DataRow {
		string dataset;
		string area;
		string item;
		string element;
		int32_t year;
		double value;
		bool has_value;
		string unit;
	};

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		string dataset;
		string element;
		std::vector<string> countries;

		explicit BindData(const string &dataset, const string &element, const std::vector<string> &countries)
		    : dataset(dataset), element(element), countries(std::move(countries)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 2);
		const string dataset = StringValue::Get(input.inputs[0]);
		const string element = StringValue::Get(input.inputs[1]);

		if (dataset.empty()) {
			throw InvalidInputException("SUDAN: The dataset parameter cannot be empty for SUDAN_FAO().");
		}
		if (element.empty()) {
			throw InvalidInputException("SUDAN: The element parameter cannot be empty for SUDAN_FAO().");
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

		names.emplace_back("dataset");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("area");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("item");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("element");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("year");
		return_types.push_back(LogicalType::INTEGER);
		names.emplace_back("value");
		return_types.push_back(LogicalType::DOUBLE);
		names.emplace_back("unit");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(dataset, element, countries);
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

	//! Map ISO3 country code to FAO area code
	static string GetFAOAreaCode(const string &iso3) {
		// FAO uses numeric area codes
		static const std::unordered_map<string, string> fao_codes = {
		    {"SDN", "276"}, {"EGY", "59"},  {"ETH", "238"}, {"TCD", "39"},
		    {"SSD", "277"}, {"ERI", "178"}, {"LBY", "124"}, {"CAF", "37"},
		};
		auto it = fao_codes.find(iso3);
		return it != fao_codes.end() ? it->second : iso3;
	}

	static void FetchFAOData(const HttpSettings &settings, const string &dataset, const string &element,
	                         const string &country_iso3, std::vector<DataRow> &rows) {

		string area_code = GetFAOAreaCode(country_iso3);

		// FAOSTAT API endpoint
		string url = "https://fenixservices.fao.org/faostat/api/v1/en/data/" + dataset +
		             "?area=" + area_code + "&output_type=objects";

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
		auto data_arr = yyjson_obj_get(root_val, "data");
		if (!yyjson_is_arr(data_arr)) {
			yyjson_doc_free(json_data);
			return;
		}

		string element_lower = StringUtil::Lower(element);
		auto arr_len = yyjson_arr_size(data_arr);

		for (size_t i = 0; i < arr_len; i++) {
			auto elem = yyjson_arr_get(data_arr, i);

			// Filter by element name
			auto elem_val = yyjson_obj_get(elem, "Element");
			if (yyjson_is_str(elem_val)) {
				string elem_name = yyjson_get_str(elem_val);
				string elem_name_lower = StringUtil::Lower(elem_name);
				// Match on element name (case-insensitive, partial match)
				if (elem_name_lower.find(element_lower) == string::npos) {
					continue;
				}
			}

			DataRow row;
			row.dataset = dataset;
			row.has_value = false;

			auto area_val = yyjson_obj_get(elem, "Area");
			if (yyjson_is_str(area_val)) {
				row.area = yyjson_get_str(area_val);
			}

			auto item_val = yyjson_obj_get(elem, "Item");
			if (yyjson_is_str(item_val)) {
				row.item = yyjson_get_str(item_val);
			}

			if (yyjson_is_str(elem_val)) {
				row.element = yyjson_get_str(elem_val);
			}

			auto year_val = yyjson_obj_get(elem, "Year");
			if (yyjson_is_int(year_val)) {
				row.year = yyjson_get_int(year_val);
			} else if (yyjson_is_str(year_val)) {
				try {
					row.year = std::stoi(yyjson_get_str(year_val));
				} catch (...) {
					row.year = 0;
				}
			}

			auto val_node = yyjson_obj_get(elem, "Value");
			if (yyjson_is_real(val_node)) {
				row.value = yyjson_get_real(val_node);
				row.has_value = true;
			} else if (yyjson_is_int(val_node)) {
				row.value = static_cast<double>(yyjson_get_int(val_node));
				row.has_value = true;
			} else if (yyjson_is_str(val_node)) {
				try {
					row.value = std::stod(yyjson_get_str(val_node));
					row.has_value = true;
				} catch (...) {
				}
			}

			auto unit_val = yyjson_obj_get(elem, "Unit");
			if (yyjson_is_str(unit_val)) {
				row.unit = yyjson_get_str(unit_val);
			}

			rows.push_back(row);
		}

		yyjson_doc_free(json_data);
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &state = global_state->Cast<State>();

		HttpSettings settings = HttpClient::ExtractHttpSettings(context, "https://fenixservices.fao.org");
		settings.timeout = 90;

		for (const auto &country : bind_data.countries) {
			FetchFAOData(settings, bind_data.dataset, bind_data.element, country, state.rows);
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
			output.data[0].SetValue(row_idx, row.dataset);
			output.data[1].SetValue(row_idx, row.area);
			output.data[2].SetValue(row_idx, row.item);
			output.data[3].SetValue(row_idx, row.element);
			output.data[4].SetValue(row_idx, Value::INTEGER(row.year));
			if (row.has_value) {
				output.data[5].SetValue(row_idx, Value::DOUBLE(row.value));
			} else {
				output.data[5].SetValue(row_idx, Value());
			}
			output.data[6].SetValue(row_idx, row.unit.empty() ? Value() : Value(row.unit));
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Reads FAO (FAOSTAT) agricultural data for Sudan and neighboring countries.
		Requires a dataset code (e.g., 'QCL' for crop production) and element name (e.g., 'production_quantity').
	)";

	static constexpr auto EXAMPLE = R"(
		-- Sudan crop production
		SELECT * FROM SUDAN_FAO('QCL', 'production_quantity')
		WHERE item = 'Wheat';

		-- Compare with neighbors
		SELECT * FROM SUDAN_FAO('QCL', 'production_quantity', countries := ['SDN', 'EGY', 'ETH']);
	)";

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_FAO", {LogicalType::VARCHAR, LogicalType::VARCHAR}, Execute, Bind, Init);
		func.named_parameters["countries"] = LogicalType::LIST(LogicalType::VARCHAR);

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

//======================================================================================================================
// Register FAO Functions
//======================================================================================================================

void FAOFunctions::Register(ExtensionLoader &loader) {
	SudanFAO::Register(loader);
}

} // namespace duckdb
