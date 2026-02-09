#include "wb_indicators.hpp"
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
#include "sudan/http_client.hpp"
#include "sudan/cache.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// SUDAN_WB_Indicators
//======================================================================================================================

struct SudanWBIndicators {

	struct IndicatorInfo {
		string id;
		string name;
		string source;
		string source_note;
	};

	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

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

		names.emplace_back("indicator_id");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("indicator_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("source");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("source_note");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>(search);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init
	//------------------------------------------------------------------------------------------------------------------

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

		// Fetch indicator list from World Bank
		int page = 1;
		int total_pages = 1;
		HttpSettings settings = HttpClient::ExtractHttpSettings(context, "https://api.worldbank.org");
		settings.timeout = 90;

		while (page <= total_pages) {
			string url = "https://api.worldbank.org/v2/indicator?format=json&per_page=1000&page=" +
			             std::to_string(page);

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

			auto json_data = yyjson_read(body.c_str(), body.size(), YYJSON_READ_NOFLAG);
			if (!json_data) {
				break;
			}

			auto root_val = yyjson_doc_get_root(json_data);
			if (!yyjson_is_arr(root_val) || yyjson_arr_size(root_val) < 2) {
				yyjson_doc_free(json_data);
				break;
			}

			// Parse pagination
			auto meta = yyjson_arr_get(root_val, 0);
			if (yyjson_is_obj(meta)) {
				auto pages_val = yyjson_obj_get(meta, "pages");
				if (yyjson_is_int(pages_val)) {
					total_pages = yyjson_get_int(pages_val);
				}
			}

			auto data_arr = yyjson_arr_get(root_val, 1);
			if (yyjson_is_arr(data_arr)) {
				string search_lower = StringUtil::Lower(bind_data.search);
				auto arr_len = yyjson_arr_size(data_arr);

				for (size_t i = 0; i < arr_len; i++) {
					auto elem = yyjson_arr_get(data_arr, i);

					IndicatorInfo info;
					auto id_val = yyjson_obj_get(elem, "id");
					auto name_val = yyjson_obj_get(elem, "name");
					auto source_obj = yyjson_obj_get(elem, "source");
					auto note_val = yyjson_obj_get(elem, "sourceNote");

					if (yyjson_is_str(id_val)) {
						info.id = yyjson_get_str(id_val);
					}
					if (yyjson_is_str(name_val)) {
						info.name = yyjson_get_str(name_val);
					}
					if (yyjson_is_obj(source_obj)) {
						auto src_val = yyjson_obj_get(source_obj, "value");
						if (yyjson_is_str(src_val)) {
							info.source = yyjson_get_str(src_val);
						}
					}
					if (yyjson_is_str(note_val)) {
						info.source_note = yyjson_get_str(note_val);
					}

					// Filter by search term if provided
					if (!search_lower.empty()) {
						string name_lower = StringUtil::Lower(info.name);
						string id_lower = StringUtil::Lower(info.id);
						if (name_lower.find(search_lower) == string::npos &&
						    id_lower.find(search_lower) == string::npos) {
							continue;
						}
					}

					state.rows.push_back(info);
				}
			}

			yyjson_doc_free(json_data);
			page++;
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
			const auto &info = state.rows[state.current_row + row_idx];
			output.data[0].SetValue(row_idx, info.id);
			output.data[1].SetValue(row_idx, info.name);
			output.data[2].SetValue(row_idx, info.source);
			if (info.source_note.empty()) {
				output.data[3].SetValue(row_idx, Value());
			} else {
				output.data[3].SetValue(row_idx, info.source_note);
			}
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Lists World Bank indicators. Optionally filter by search term.
	)";

	static constexpr auto EXAMPLE = R"(
		-- List all indicators
		SELECT * FROM SUDAN_WB_Indicators() LIMIT 10;

		-- Search for population indicators
		SELECT * FROM SUDAN_WB_Indicators(search := 'population');

		+-----------------+------------------------------+----------------------------+
		| indicator_id    | indicator_name               | source                     |
		+-----------------+------------------------------+----------------------------+
		| SP.POP.TOTL     | Population, total            | World Development Indicators|
		| SP.POP.GROW     | Population growth (annual %) | World Development Indicators|
		+-----------------+------------------------------+----------------------------+
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_WB_Indicators", {}, Execute, Bind, Init);
		func.named_parameters["search"] = LogicalType::VARCHAR;

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

//======================================================================================================================
// Register World Bank Indicator Functions
//======================================================================================================================

void WorldBankIndicatorFunctions::Register(ExtensionLoader &loader) {
	SudanWBIndicators::Register(loader);
}

} // namespace duckdb
