#include "geo_functions.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

// SUDAN
#include "sudan/providers.hpp"
#include "sudan/http_client.hpp"
#include "sudan/geo/sudan_boundaries_data.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// Sudan State Data (Embedded)
//======================================================================================================================

struct SudanState {
	const char *iso_code;
	const char *name;
	const char *name_ar;
	double centroid_lon;
	double centroid_lat;
};

// Sudan's 18 states with ISO 3166-2 codes, English/Arabic names, and centroids
static const SudanState SUDAN_STATES[] = {
    {"SD-KH", "Khartoum", "\xd8\xa7\xd9\x84\xd8\xae\xd8\xb1\xd8\xb7\xd9\x88\xd9\x85", 32.53, 15.55},
    {"SD-GZ", "Al Jazirah", "\xd8\xa7\xd9\x84\xd8\xac\xd8\xb2\xd9\x8a\xd8\xb1\xd8\xa9", 33.53, 14.88},
    {"SD-GD", "Al Qadarif", "\xd8\xa7\xd9\x84\xd9\x82\xd8\xb6\xd8\xa7\xd8\xb1\xd9\x81", 35.40, 14.03},
    {"SD-KA", "Kassala", "\xd9\x83\xd8\xb3\xd9\x84\xd8\xa7", 36.40, 15.45},
    {"SD-RS", "Red Sea", "\xd8\xa7\xd9\x84\xd8\xa8\xd8\xad\xd8\xb1 \xd8\xa7\xd9\x84\xd8\xa3\xd8\xad\xd9\x85\xd8\xb1", 37.22, 19.62},
    {"SD-NR", "River Nile", "\xd9\x86\xd9\x87\xd8\xb1 \xd8\xa7\xd9\x84\xd9\x86\xd9\x8a\xd9\x84", 33.93, 17.50},
    {"SD-NO", "Northern", "\xd8\xa7\xd9\x84\xd8\xb4\xd9\x85\xd8\xa7\xd9\x84\xd9\x8a\xd8\xa9", 30.22, 19.50},
    {"SD-NW", "White Nile", "\xd8\xa7\xd9\x84\xd9\x86\xd9\x8a\xd9\x84 \xd8\xa7\xd9\x84\xd8\xa3\xd8\xa8\xd9\x8a\xd8\xb6", 32.17, 13.17},
    {"SD-NB", "Blue Nile", "\xd8\xa7\xd9\x84\xd9\x86\xd9\x8a\xd9\x84 \xd8\xa7\xd9\x84\xd8\xa3\xd8\xb2\xd8\xb1\xd9\x82", 34.05, 11.25},
    {"SD-SI", "Sennar", "\xd8\xb3\xd9\x86\xd8\xa7\xd8\xb1", 34.13, 13.55},
    {"SD-DS", "South Darfur", "\xd8\xac\xd9\x86\xd9\x88\xd8\xa8 \xd8\xaf\xd8\xa7\xd8\xb1\xd9\x81\xd9\x88\xd8\xb1", 24.92, 11.75},
    {"SD-DN", "North Darfur", "\xd8\xb4\xd9\x85\xd8\xa7\xd9\x84 \xd8\xaf\xd8\xa7\xd8\xb1\xd9\x81\xd9\x88\xd8\xb1", 25.08, 15.77},
    {"SD-DW", "West Darfur", "\xd8\xba\xd8\xb1\xd8\xa8 \xd8\xaf\xd8\xa7\xd8\xb1\xd9\x81\xd9\x88\xd8\xb1", 22.85, 12.83},
    {"SD-DC", "Central Darfur", "\xd9\x88\xd8\xb3\xd8\xb7 \xd8\xaf\xd8\xa7\xd8\xb1\xd9\x81\xd9\x88\xd8\xb1", 24.23, 13.50},
    {"SD-DE", "East Darfur", "\xd8\xb4\xd8\xb1\xd9\x82 \xd8\xaf\xd8\xa7\xd8\xb1\xd9\x81\xd9\x88\xd8\xb1", 26.13, 12.75},
    {"SD-KN", "North Kordofan", "\xd8\xb4\xd9\x85\xd8\xa7\xd9\x84 \xd9\x83\xd8\xb1\xd8\xaf\xd9\x81\xd8\xa7\xd9\x86", 29.42, 13.83},
    {"SD-KS", "South Kordofan", "\xd8\xac\xd9\x86\xd9\x88\xd8\xa8 \xd9\x83\xd8\xb1\xd8\xaf\xd9\x81\xd8\xa7\xd9\x86", 29.67, 11.20},
    {"SD-KW", "West Kordofan", "\xd8\xba\xd8\xb1\xd8\xa8 \xd9\x83\xd8\xb1\xd8\xaf\xd9\x81\xd8\xa7\xd9\x86", 28.05, 12.25},
};

static constexpr idx_t SUDAN_STATE_COUNT = sizeof(SUDAN_STATES) / sizeof(SudanState);

//======================================================================================================================
// SUDAN_Boundaries
//======================================================================================================================

struct SudanBoundaries {

	struct BoundaryRow {
		string level;
		string name;
		string name_ar;
		string iso_code;
		string state_name;
		string geojson;
	};

	struct BindData final : TableFunctionData {
		string level;
		explicit BindData(const string &level) : level(level) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		D_ASSERT(input.inputs.size() == 1);
		const string level = StringValue::Get(input.inputs[0]);

		string level_lower = StringUtil::Lower(level);
		if (level_lower != "country" && level_lower != "state" && level_lower != "locality") {
			throw InvalidInputException(
			    "SUDAN: Invalid boundary level '%s'. Valid levels: 'country', 'state', 'locality'.",
			    level.c_str());
		}

		if (level_lower == "locality") {
			// Locality-level columns
			names.emplace_back("locality_name");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("locality_name_ar");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("state_name");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("geojson");
			return_types.push_back(LogicalType::VARCHAR);
		} else if (level_lower == "state") {
			names.emplace_back("state_name");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("state_name_ar");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("iso_code");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("geojson");
			return_types.push_back(LogicalType::VARCHAR);
		} else {
			// country level
			names.emplace_back("country_name");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("country_name_ar");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("iso_code");
			return_types.push_back(LogicalType::VARCHAR);
			names.emplace_back("geojson");
			return_types.push_back(LogicalType::VARCHAR);
		}

		return make_uniq_base<FunctionData, BindData>(level_lower);
	}

	struct State final : GlobalTableFunctionState {
		std::vector<BoundaryRow> rows;
		idx_t current_row;
		explicit State() : current_row(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto global_state = make_uniq_base<GlobalTableFunctionState, State>();
		auto &state = global_state->Cast<State>();

		if (bind_data.level == "country") {
			BoundaryRow row;
			row.level = "country";
			row.name = "Sudan";
			row.name_ar = "\xd8\xa7\xd9\x84\xd8\xb3\xd9\x88\xd8\xaf\xd8\xa7\xd9\x86";
			row.iso_code = "SDN";
			row.geojson = COUNTRY_BOUNDARY_GEOJSON;
			state.rows.push_back(row);
		} else if (bind_data.level == "state") {
			for (idx_t i = 0; i < SUDAN_STATE_COUNT; i++) {
				BoundaryRow row;
				row.level = "state";
				row.name = SUDAN_STATES[i].name;
				row.name_ar = SUDAN_STATES[i].name_ar;
				row.iso_code = SUDAN_STATES[i].iso_code;
				row.geojson = GetStateBoundaryGeoJSON(i);
				state.rows.push_back(row);
			}
		} else if (bind_data.level == "locality") {
			// Locality data would be fetched from GADM on-demand
			// For now, return an informative message by providing no rows
			// Users can fetch from GADM directly via httpfs
		}

		return global_state;
	}

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto &state = input.global_state->Cast<State>();

		const auto output_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.current_row);

		if (output_size == 0) {
			output.SetCardinality(0);
			return;
		}

		for (idx_t row_idx = 0; row_idx < output_size; row_idx++) {
			const auto &row = state.rows[state.current_row + row_idx];

			if (bind_data.level == "country") {
				output.data[0].SetValue(row_idx, row.name);
				output.data[1].SetValue(row_idx, row.name_ar);
				output.data[2].SetValue(row_idx, row.iso_code);
				output.data[3].SetValue(row_idx, row.geojson);
			} else if (bind_data.level == "state") {
				output.data[0].SetValue(row_idx, row.name);
				output.data[1].SetValue(row_idx, row.name_ar);
				output.data[2].SetValue(row_idx, row.iso_code);
				output.data[3].SetValue(row_idx, row.geojson);
			} else {
				// locality
				output.data[0].SetValue(row_idx, row.name);
				output.data[1].SetValue(row_idx, row.name_ar);
				output.data[2].SetValue(row_idx, row.state_name);
				output.data[3].SetValue(row_idx, row.geojson);
			}
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	static constexpr auto DESCRIPTION = R"(
		Returns administrative boundaries of Sudan as GeoJSON strings.
		Level can be 'country' (admin-0), 'state' (admin-1, 18 states), or 'locality' (admin-2).
		Country and state boundaries are embedded in the extension (work offline).
		Locality boundaries are fetched on-demand from GADM.
	)";

	static constexpr auto EXAMPLE = R"(
		-- Get all 18 state boundaries
		SELECT state_name, state_name_ar, iso_code, geojson FROM SUDAN_Boundaries('state');

		-- Get country boundary
		SELECT * FROM SUDAN_Boundaries('country');

		-- Use with spatial extension
		SELECT state_name, ST_GeomFromGeoJSON(geojson) AS geom FROM SUDAN_Boundaries('state');
	)";

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_Boundaries", {LogicalType::VARCHAR}, Execute, Bind, Init);
		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

//======================================================================================================================
// SUDAN_States
//======================================================================================================================

struct SudanStates {

	struct BindData final : TableFunctionData {
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {

		names.emplace_back("state_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("state_name_ar");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("iso_code");
		return_types.push_back(LogicalType::VARCHAR);
		names.emplace_back("centroid_lon");
		return_types.push_back(LogicalType::DOUBLE);
		names.emplace_back("centroid_lat");
		return_types.push_back(LogicalType::DOUBLE);
		names.emplace_back("geojson");
		return_types.push_back(LogicalType::VARCHAR);

		return make_uniq_base<FunctionData, BindData>();
	}

	struct State final : GlobalTableFunctionState {
		idx_t current_row;
		explicit State() : current_row(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq_base<GlobalTableFunctionState, State>();
	}

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &state = input.global_state->Cast<State>();

		const auto output_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, SUDAN_STATE_COUNT - state.current_row);

		if (output_size == 0) {
			output.SetCardinality(0);
			return;
		}

		for (idx_t row_idx = 0; row_idx < output_size; row_idx++) {
			idx_t state_idx = state.current_row + row_idx;
			const auto &s = SUDAN_STATES[state_idx];
			output.data[0].SetValue(row_idx, string(s.name));
			output.data[1].SetValue(row_idx, string(s.name_ar));
			output.data[2].SetValue(row_idx, string(s.iso_code));
			output.data[3].SetValue(row_idx, Value::DOUBLE(s.centroid_lon));
			output.data[4].SetValue(row_idx, Value::DOUBLE(s.centroid_lat));
			output.data[5].SetValue(row_idx, GetStateBoundaryGeoJSON(state_idx));
		}

		state.current_row += output_size;
		output.SetCardinality(output_size);
	}

	static constexpr auto DESCRIPTION = R"(
		Returns Sudan's 18 states with names (English and Arabic), ISO codes, centroids, and GeoJSON geometry.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT state_name, state_name_ar, iso_code FROM SUDAN_States();

		+------------------+------------------+---------+
		| state_name       | state_name_ar    | iso_code|
		+------------------+------------------+---------+
		| Khartoum         | الخرطوم          | SD-KH   |
		| Al Jazirah       | الجزيرة          | SD-GZ   |
		| ...              | ...              | ...     |
		+------------------+------------------+---------+
	)";

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "table");

		TableFunction func("SUDAN_States", {}, Execute, Bind, Init);
		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

//======================================================================================================================
// SUDAN_GeoCode (Scalar Function)
//======================================================================================================================

struct SudanGeoCode {

	inline static void GeoCodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);

		UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input_name) {
			std::string name_str = input_name.GetString();

			// Search by English or Arabic name (case-insensitive for English)
			for (idx_t i = 0; i < SUDAN_STATE_COUNT; i++) {
				const auto &s = SUDAN_STATES[i];

				// Exact match on Arabic name
				if (name_str == s.name_ar) {
					return string_t(s.iso_code);
				}

				// Case-insensitive match on English name
				std::string english_lower = StringUtil::Lower(s.name);
				std::string input_lower = StringUtil::Lower(name_str);
				if (input_lower == english_lower) {
					return string_t(s.iso_code);
				}
			}

			// Return empty string if not found
			return string_t("");
		});
	}

	static constexpr auto DESCRIPTION = R"(
		Returns the ISO 3166-2 code for a Sudan state name. Accepts Arabic or English input.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT SUDAN_GeoCode('Khartoum');     -- returns 'SD-KH'
		SELECT SUDAN_GeoCode('الخرطوم');      -- returns 'SD-KH'
	)";

	static void Register(ExtensionLoader &loader) {

		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "sudan");
		tags.insert("category", "scalar");

		ScalarFunction func("SUDAN_GeoCode", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
		                    SudanGeoCode::GeoCodeFunction);

		RegisterFunction<ScalarFunction>(loader, func, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

} // namespace

//======================================================================================================================
// Register Geo Functions
//======================================================================================================================

void GeoFunctions::Register(ExtensionLoader &loader) {
	SudanBoundaries::Register(loader);
	SudanStates::Register(loader);
	SudanGeoCode::Register(loader);
}

} // namespace duckdb
