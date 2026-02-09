#include "info_functions.hpp"
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

namespace duckdb {

namespace {

//======================================================================================================================
// SUDAN_Providers
//======================================================================================================================

struct SudanProviders {

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		idx_t provider_count;
		explicit BindData(const idx_t count_p) : provider_count(count_p) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		names.emplace_back("provider_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("name_ar");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("description");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("base_url");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(sudan::PROVIDERS.size());
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		idx_t current_idx;
		explicit State() : current_idx(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq_base<GlobalTableFunctionState, State>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto &state = input.global_state->Cast<State>();

		idx_t count = 0;
		auto next_idx = MinValue<idx_t>(state.current_idx + STANDARD_VECTOR_SIZE, bind_data.provider_count);

		for (; state.current_idx < next_idx; state.current_idx++) {
			const auto &provider = sudan::PROVIDERS[state.current_idx];

			output.data[0].SetValue(count, provider.id);
			output.data[1].SetValue(count, provider.name);
			output.data[2].SetValue(count, provider.name_ar);
			output.data[3].SetValue(count, provider.description);
			output.data[4].SetValue(count, provider.base_url);
			count++;
		}
		output.SetCardinality(count);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns the list of supported data providers for Sudan data.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT provider_id, name, description FROM SUDAN_Providers();

		+-----------+-----------------------------+----------------------------------------------+
		|provider_id|           name              |                 description                  |
		+-----------+-----------------------------+----------------------------------------------+
		| worldbank | World Bank                  | World Development Indicators and other ...   |
		| who       | World Health Organization   | Global Health Observatory (GHO) data         |
		| fao       | Food and Agriculture Org... | FAOSTAT agricultural statistics               |
		| unhcr     | UNHCR                       | UN Refugee Agency displacement and pop...    |
		| ilo       | International Labour Org... | International Labour Organization statistics |
		+-----------+-----------------------------+----------------------------------------------+
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		const TableFunction func("SUDAN_Providers", {}, Execute, Bind, Init);
		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

//======================================================================================================================
// SUDAN_Search
//======================================================================================================================

struct SudanSearch {

	//! Search result row
	struct SearchResult {
		string provider;
		string indicator_id;
		string indicator_name;
	};

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		string query;

		explicit BindData(const string &query) : query(query) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		string query;

		// Extract query from named parameters
		auto options_param = input.named_parameters.find("query");
		if (options_param != input.named_parameters.end()) {
			auto &item = options_param->second;
			if (!item.IsNull() && item.type() == LogicalType::VARCHAR) {
				query = item.GetValue<string>();
			}
		}

		if (query.empty()) {
			throw InvalidInputException("SUDAN: The 'query' parameter is required for SUDAN_Search().");
		}

		names.emplace_back("provider");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("indicator_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("indicator_name");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(query);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		std::vector<SearchResult> rows;
		idx_t current_row;

		explicit State() : current_row(0) {
		}
	};

	//! Search World Bank indicators for matching keywords
	static void SearchWorldBank(const HttpSettings &settings, const string &query, std::vector<SearchResult> &results) {
		string url = "https://api.worldbank.org/v2/indicator?format=json&per_page=1000&source=2";

		auto response = HttpClient::Get(settings, url);
		if (response.status_code != 200 || !response.error.empty()) {
			return;
		}

		auto json_data = yyjson_read(response.body.c_str(), response.body.size(), YYJSON_READ_NOFLAG);
		if (!json_data) {
			return;
		}

		auto root_val = yyjson_doc_get_root(json_data);
		if (!yyjson_is_arr(root_val) || yyjson_arr_size(root_val) < 2) {
			yyjson_doc_free(json_data);
			return;
		}

		auto data_arr = yyjson_arr_get(root_val, 1);
		if (!yyjson_is_arr(data_arr)) {
			yyjson_doc_free(json_data);
			return;
		}

		string query_lower = StringUtil::Lower(query);
		auto arr_len = yyjson_arr_size(data_arr);

		for (size_t i = 0; i < arr_len; i++) {
			auto elem = yyjson_arr_get(data_arr, i);
			auto id_val = yyjson_obj_get(elem, "id");
			auto name_val = yyjson_obj_get(elem, "name");

			if (!yyjson_is_str(id_val) || !yyjson_is_str(name_val)) {
				continue;
			}

			string id = yyjson_get_str(id_val);
			string name = yyjson_get_str(name_val);
			string name_lower = StringUtil::Lower(name);
			string id_lower = StringUtil::Lower(id);

			if (name_lower.find(query_lower) != string::npos || id_lower.find(query_lower) != string::npos) {
				SearchResult result;
				result.provider = "worldbank";
				result.indicator_id = id;
				result.indicator_name = name;
				results.push_back(result);
			}
		}

		yyjson_doc_free(json_data);
	}

	//! Search WHO indicators for matching keywords
	static void SearchWHO(const HttpSettings &settings, const string &query, std::vector<SearchResult> &results) {
		string url = "https://ghoapi.azureedge.net/api/Indicator";

		auto response = HttpClient::Get(settings, url);
		if (response.status_code != 200 || !response.error.empty()) {
			return;
		}

		auto json_data = yyjson_read(response.body.c_str(), response.body.size(), YYJSON_READ_NOFLAG);
		if (!json_data) {
			return;
		}

		auto root_val = yyjson_doc_get_root(json_data);
		auto value_arr = yyjson_obj_get(root_val, "value");
		if (!yyjson_is_arr(value_arr)) {
			yyjson_doc_free(json_data);
			return;
		}

		string query_lower = StringUtil::Lower(query);
		auto arr_len = yyjson_arr_size(value_arr);

		for (size_t i = 0; i < arr_len; i++) {
			auto elem = yyjson_arr_get(value_arr, i);
			auto code_val = yyjson_obj_get(elem, "IndicatorCode");
			auto name_val = yyjson_obj_get(elem, "IndicatorName");

			if (!yyjson_is_str(code_val) || !yyjson_is_str(name_val)) {
				continue;
			}

			string code = yyjson_get_str(code_val);
			string name = yyjson_get_str(name_val);
			string name_lower = StringUtil::Lower(name);
			string code_lower = StringUtil::Lower(code);

			if (name_lower.find(query_lower) != string::npos || code_lower.find(query_lower) != string::npos) {
				SearchResult result;
				result.provider = "who";
				result.indicator_id = code;
				result.indicator_name = name;
				results.push_back(result);
			}
		}

		yyjson_doc_free(json_data);
	}

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &state = global_state->Cast<State>();

		// Extract settings once
		HttpSettings settings = HttpClient::ExtractHttpSettings(context, "https://api.worldbank.org");

		// Search across providers
		SearchWorldBank(settings, bind_data.query, state.rows);
		SearchWHO(settings, bind_data.query, state.rows);

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
			const auto &result = state.rows[state.current_row + row_idx];
			output.data[0].SetValue(row_idx, result.provider);
			output.data[1].SetValue(row_idx, result.indicator_id);
			output.data[2].SetValue(row_idx, result.indicator_name);
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Searches for indicators across all supported data providers matching the given query.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT * FROM SUDAN_Search(query := 'maternal mortality');

		+-----------+-----------------+------------------------------------------+
		| provider  | indicator_id    | indicator_name                           |
		+-----------+-----------------+------------------------------------------+
		| worldbank | SH.STA.MMRT     | Maternal mortality ratio (per 100,000)   |
		| who       | MDG_0000000025  | Maternal mortality ratio (per 100 000)   |
		+-----------+-----------------+------------------------------------------+
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_Search", {}, Execute, Bind, Init);
		func.named_parameters["query"] = LogicalType::VARCHAR;

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

//======================================================================================================================
// Register Info Functions
//======================================================================================================================

void InfoFunctions::Register(ExtensionLoader &loader) {
	SudanProviders::Register(loader);
	SudanSearch::Register(loader);
}

} // namespace duckdb
