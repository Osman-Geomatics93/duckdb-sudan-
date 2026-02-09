# DuckDB Sudan Extension

A DuckDB extension that provides unified SQL access to Sudan's publicly available data across multiple international APIs.

Sudan is experiencing one of the world's largest humanitarian crises, yet its data is scattered across dozens of international APIs. This extension brings **all publicly available Sudan data into DuckDB** — enabling analysts, researchers, and humanitarian workers to query demographics, economics, health, agriculture, displacement, and geospatial boundaries using standard SQL.

## Data Providers

| Provider | Function | Data |
|----------|----------|------|
| World Bank | `SUDAN_WorldBank()` | Development indicators, GDP, population, education |
| WHO | `SUDAN_WHO()` | Health indicators, disease burden, mortality |
| FAO | `SUDAN_FAO()` | Agricultural production, food security, land use |
| UNHCR | `SUDAN_UNHCR()` | Refugees, IDPs, asylum seekers, displacement |
| ILO | `SUDAN_ILO()` | Employment, labor force, unemployment |

## Quick Start

```sql
-- Load the extension
LOAD sudan;

-- List all available data providers
SELECT * FROM SUDAN_Providers();

-- Sudan population over time
SELECT year, value FROM SUDAN_WorldBank('SP.POP.TOTL')
WHERE year >= 2000
ORDER BY year;

-- Compare Sudan with neighbors
SELECT country_name, year, value
FROM SUDAN_WorldBank('SP.POP.TOTL', countries := ['SDN', 'EGY', 'ETH', 'SSD'])
WHERE year = 2023;

-- UNHCR displacement data
SELECT * FROM SUDAN_UNHCR('idps');

-- Search indicators across all providers
SELECT * FROM SUDAN_Search(query := 'maternal mortality');

-- Get Sudan's 18 states with Arabic names
SELECT state_name, state_name_ar, iso_code FROM SUDAN_States();

-- Lookup state code from Arabic name
SELECT SUDAN_GeoCode('الخرطوم');  -- returns 'SD-KH'
```

## Supported Countries

All data functions default to Sudan but support neighboring countries for comparison:

| ISO3 | Country | Arabic |
|------|---------|--------|
| SDN | Sudan | السودان |
| EGY | Egypt | مصر |
| ETH | Ethiopia | إثيوبيا |
| TCD | Chad | تشاد |
| SSD | South Sudan | جنوب السودان |
| ERI | Eritrea | إريتريا |
| LBY | Libya | ليبيا |
| CAF | Central African Republic | جمهورية أفريقيا الوسطى |

## Geospatial Functions

The extension includes embedded geographic data for Sudan:

```sql
-- Get all 18 state boundaries (works offline)
SELECT state_name, state_name_ar, iso_code, geojson
FROM SUDAN_Boundaries('state');

-- Use with DuckDB's spatial extension
SELECT state_name, ST_GeomFromGeoJSON(geojson) AS geom
FROM SUDAN_States();
```

## Building from Source

```bash
git clone --recurse-submodules https://github.com/<user>/duckdb-sudan
cd duckdb-sudan
GEN=ninja make release    # Build
make test                 # Run SQL tests
./build/release/duckdb    # Launch with extension loaded
```

### Dependencies

- CMake 3.5+
- C++17 compatible compiler
- OpenSSL (for HTTPS)
- DuckDB (included as submodule)

## Function Reference

See [docs/functions.md](docs/functions.md) for the complete function reference.

## License

MIT License
