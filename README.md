<div align="center">

# DuckDB Sudan Extension

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![DuckDB](https://img.shields.io/badge/DuckDB-v1.2+-blue.svg)](https://duckdb.org)
[![Docs](https://img.shields.io/badge/Docs-Live-green.svg)](https://osman-geomatics93.github.io/duckdb-sudan-/)
[![SQL Functions](https://img.shields.io/badge/SQL_Functions-12-orange.svg)](#all-sql-functions)
[![APIs](https://img.shields.io/badge/APIs-5-purple.svg)](#data-providers)

**Unified SQL access to Sudan's humanitarian, development, and geospatial data from 5 international APIs**

[**Explore the Documentation**](https://osman-geomatics93.github.io/duckdb-sudan-/) | [**View on GitHub**](https://github.com/Osman-Geomatics93/duckdb-sudan-) | [**Report Bug**](https://github.com/Osman-Geomatics93/duckdb-sudan-/issues)

---

</div>

Sudan is experiencing one of the world's largest humanitarian crises, yet its data is scattered across dozens of international APIs with incompatible formats and authentication schemes. This extension brings **all publicly available Sudan data into DuckDB** — enabling analysts, researchers, and humanitarian workers to query demographics, economics, health, agriculture, displacement, and geospatial boundaries using standard SQL.

> **Interactive Documentation** — Visit [**osman-geomatics93.github.io/duckdb-sudan-**](https://osman-geomatics93.github.io/duckdb-sudan-/) for live charts, interactive map, SQL playground, and bilingual (Arabic/English) reference.

![DuckDB Sudan Extension Demo](docs/demo.png)

## Data Providers

| Provider | Function | Data | Rows (Sudan) |
|----------|----------|------|:------------:|
| [World Bank](https://data.worldbank.org/) | `SUDAN_WorldBank()` | Development indicators, GDP, population, education | 65+ |
| [WHO](https://www.who.int/data/gho) | `SUDAN_WHO()` | Health indicators, disease burden, mortality | 39+ |
| [FAO](https://www.fao.org/faostat/) | `SUDAN_FAO()` | Agricultural production, food security, land use | 188+ |
| [UNHCR](https://www.unhcr.org/refugee-statistics/) | `SUDAN_UNHCR()` | Refugees, IDPs, asylum seekers, displacement | 122+ |
| [ILO](https://ilostat.ilo.org/) | `SUDAN_ILO()` | Employment, labor force, unemployment | 302+ |

Plus **12 SQL functions** including indicator search, geospatial boundaries, and bilingual (Arabic/English) state data.

## Quick Start

```sql
-- Load the extension (use -unsigned flag when launching DuckDB)
LOAD sudan;

-- List all available data providers
SELECT * FROM SUDAN_Providers();
```

### Population & Demographics

```sql
-- Sudan population over time
SELECT year, value FROM SUDAN_WorldBank('SP.POP.TOTL')
WHERE year >= 2000
ORDER BY year;

-- Compare Sudan with neighbors (2024 data)
SELECT country_name, year, value
FROM SUDAN_WorldBank('SP.POP.TOTL', countries := ['SDN', 'EGY', 'ETH', 'SSD'])
WHERE year = 2024
ORDER BY value DESC;
-- Ethiopia: 125M, Egypt: 112M, Sudan: 49M, South Sudan: 11M
```

### Health

```sql
-- Maternal mortality ratio over time
SELECT year, value FROM SUDAN_WHO('MDG_0000000026')
ORDER BY year;

-- Search for health indicators
SELECT * FROM SUDAN_WHO_Indicators(search := 'mortality');
```

### Displacement & Refugees

```sql
-- Internally displaced persons
SELECT year, value FROM SUDAN_UNHCR('idps')
WHERE year >= 2020
ORDER BY year DESC;

-- Sudanese refugees worldwide (2.4M+ in 2025)
SELECT year, country_asylum_name, value
FROM SUDAN_UNHCR('refugees')
WHERE year = 2025
ORDER BY value DESC;

-- Compare Sudan and South Sudan refugee data
SELECT * FROM SUDAN_UNHCR('refugees', countries := ['SDN', 'SSD']);
```

### Agriculture & Food Security

```sql
-- Crop production in Sudan
SELECT item, year, value, unit
FROM SUDAN_FAO('QCL', 'production')
WHERE year >= 2020
ORDER BY item, year DESC;

-- Compare wheat production across neighbors
SELECT area, item, year, value
FROM SUDAN_FAO('QCL', 'production', countries := ['SDN', 'EGY', 'ETH'])
WHERE item = 'Wheat' AND year >= 2020;
```

### Labor & Employment

```sql
-- Unemployment rate by sex and age
SELECT sex, classif1, year, value
FROM SUDAN_ILO('UNE_DEAP_SEX_AGE_RT')
WHERE year = 2022
ORDER BY sex, classif1;
```

### Cross-Provider Search

```sql
-- Search indicators across all providers
SELECT * FROM SUDAN_Search(query := 'maternal mortality');

-- Search World Bank indicators
SELECT * FROM SUDAN_WB_Indicators(search := 'GDP');
```

### Geospatial Data

```sql
-- Get all 18 state boundaries (embedded, works offline)
SELECT state_name, state_name_ar, iso_code, geojson
FROM SUDAN_Boundaries('state');

-- List Sudan's 18 states with bilingual names
SELECT state_name, state_name_ar, iso_code FROM SUDAN_States();

-- Lookup state code from Arabic name
SELECT SUDAN_GeoCode('الخرطوم');  -- returns 'SD-KH'

-- Use with DuckDB's spatial extension
SELECT state_name, ST_GeomFromGeoJSON(geojson) AS geom
FROM SUDAN_States();
```

## Supported Countries

All data functions default to Sudan but support neighboring countries for regional comparison:

| ISO3 | Country | Arabic |
|:----:|---------|--------|
| SDN | Sudan | السودان |
| EGY | Egypt | مصر |
| ETH | Ethiopia | إثيوبيا |
| TCD | Chad | تشاد |
| SSD | South Sudan | جنوب السودان |
| ERI | Eritrea | إريتريا |
| LBY | Libya | ليبيا |
| CAF | Central African Republic | جمهورية أفريقيا الوسطى |

```sql
-- Use countries parameter on any data function
SELECT * FROM SUDAN_WorldBank('SP.POP.TOTL', countries := ['SDN', 'EGY', 'ETH']);
SELECT * FROM SUDAN_UNHCR('refugees', countries := ['SDN', 'SSD']);
SELECT * FROM SUDAN_FAO('QCL', 'production', countries := ['SDN', 'EGY']);
```

## All SQL Functions

### Discovery & Metadata

| Function | Description |
|----------|-------------|
| `SUDAN_Providers()` | List all 5 data providers with status |
| `SUDAN_WB_Indicators(search)` | Search World Bank indicators |
| `SUDAN_WHO_Indicators(search)` | Search WHO GHO indicators |
| `SUDAN_Search(query)` | Cross-provider indicator search |

### Data

| Function | Signature | Source |
|----------|-----------|--------|
| `SUDAN_WorldBank` | `(indicator, countries := ['SDN'])` | World Bank V2 API |
| `SUDAN_WHO` | `(indicator, countries := ['SDN'])` | WHO GHO OData API |
| `SUDAN_FAO` | `(dataset, element, countries := ['SDN'])` | FAOSTAT API |
| `SUDAN_UNHCR` | `(population_type, countries := ['SDN'])` | UNHCR Population API |
| `SUDAN_ILO` | `(indicator, countries := ['SDN'])` | ILO SDMX API |

### Geospatial

| Function | Type | Description |
|----------|------|-------------|
| `SUDAN_Boundaries(level)` | Table | Admin boundaries as GeoJSON (`'country'`, `'state'`, `'locality'`) |
| `SUDAN_States()` | Table | 18 states with bilingual names, ISO codes, centroids |
| `SUDAN_GeoCode(name)` | Scalar | State name (Arabic or English) to ISO code |

See [docs/functions.md](docs/functions.md) for the complete function reference with all return columns.

## Building from Source

### Prerequisites

- CMake 3.10+
- C++17 compatible compiler (MSVC 2019+, GCC 9+, Clang 10+)
- OpenSSL (for HTTPS)
- Ninja (recommended)
- DuckDB source (included as submodule)

### Windows

```cmd
git clone --recurse-submodules https://github.com/Osman-Geomatics93/duckdb-sudan-.git
cd duckdb-sudan-
build_release.bat
```

The script sets up the Visual Studio environment, configures CMake with Ninja, and builds. Output goes to `build/release/`.

### Linux / macOS

```bash
git clone --recurse-submodules https://github.com/Osman-Geomatics93/duckdb-sudan-.git
cd duckdb-sudan-
GEN=ninja make release
make test
```

### Running

```bash
# Launch DuckDB with unsigned extension loading enabled
./build/release/duckdb -unsigned

# Inside DuckDB:
LOAD sudan;
SELECT * FROM SUDAN_Providers();
```

### Testing

```bash
# Run all 9 test cases (47 assertions)
./build/release/test/unittest "*sudan*"

# Windows
build\release\test\unittest.exe "*sudan*"
```

## Architecture

The extension follows the standard DuckDB C++ extension pattern:

- **3-phase table functions**: Bind (validate params, define schema) -> Init (fetch data via HTTP, parse JSON) -> Execute (emit rows in chunks)
- **JSON-only**: All 5 APIs return JSON, parsed with DuckDB's built-in yyjson (no XML/SDMX dependency)
- **In-memory caching**: API responses cached per session to avoid redundant network calls
- **Modular providers**: Each API has its own directory under `src/sudan/` for clean separation

```
src/
├── sudan_extension.cpp          # Entry point
├── include/
│   ├── sudan_extension.hpp      # Extension class
│   └── function_builder.hpp     # Generic function registration
└── sudan/
    ├── providers.hpp/cpp        # Provider registry & country codes
    ├── http_client.hpp/cpp      # HTTP client wrapper
    ├── cache.hpp/cpp            # Response cache
    ├── worldbank/               # World Bank API
    ├── who/                     # WHO GHO API
    ├── fao/                     # FAOSTAT API
    ├── unhcr/                   # UNHCR Population API
    ├── ilo/                     # ILO SDMX API
    ├── geo/                     # Geospatial functions
    └── info/                    # Cross-provider search
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Links

- [**Documentation Site**](https://osman-geomatics93.github.io/duckdb-sudan-/) — Interactive docs with charts, map, SQL playground
- [**GitHub Repository**](https://github.com/Osman-Geomatics93/duckdb-sudan-)
- [**Function Reference**](docs/functions.md) — Complete API documentation

## Acknowledgments

- Built on [DuckDB](https://duckdb.org/) and inspired by the [duckdb-eurostat](https://github.com/ahuarte47/duckdb-eurostat) extension
- Data sourced from: [World Bank](https://data.worldbank.org/), [WHO GHO](https://www.who.int/data/gho), [FAOSTAT](https://www.fao.org/faostat/), [UNHCR](https://www.unhcr.org/refugee-statistics/), [ILOSTAT](https://ilostat.ilo.org/)
