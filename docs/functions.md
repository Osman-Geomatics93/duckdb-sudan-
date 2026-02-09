# DuckDB Sudan Extension - Function Reference

## Discovery & Metadata Functions

### `SUDAN_Providers()`
Returns the list of all supported data providers.

**Returns:** `provider_id VARCHAR, name VARCHAR, name_ar VARCHAR, description VARCHAR, base_url VARCHAR`

```sql
SELECT * FROM SUDAN_Providers();
```

### `SUDAN_WB_Indicators(search := 'term')`
Search/list World Bank indicators. Without search parameter, lists all indicators.

**Named Parameters:**
- `search` (VARCHAR, optional) — Filter indicators by keyword

**Returns:** `indicator_id VARCHAR, indicator_name VARCHAR, source VARCHAR, source_note VARCHAR`

```sql
SELECT * FROM SUDAN_WB_Indicators(search := 'population');
```

### `SUDAN_WHO_Indicators(search := 'term')`
Search/list WHO Global Health Observatory indicator codes.

**Named Parameters:**
- `search` (VARCHAR, optional) — Filter indicators by keyword

**Returns:** `indicator_code VARCHAR, indicator_name VARCHAR, language VARCHAR`

```sql
SELECT * FROM SUDAN_WHO_Indicators(search := 'mortality');
```

### `SUDAN_Search(query := 'term')`
Cross-provider keyword search across World Bank and WHO indicator catalogs.

**Named Parameters:**
- `query` (VARCHAR, required) — Search term

**Returns:** `provider VARCHAR, indicator_id VARCHAR, indicator_name VARCHAR`

```sql
SELECT * FROM SUDAN_Search(query := 'maternal mortality');
```

---

## Data Reading Functions

### `SUDAN_WorldBank(indicator)`
Reads World Bank indicator data for Sudan (and optionally neighboring countries).

**Positional Parameters:**
- `indicator` (VARCHAR, required) — World Bank indicator code (e.g., 'SP.POP.TOTL')

**Named Parameters:**
- `countries` (VARCHAR[], optional) — List of ISO3 country codes. Default: `['SDN']`

**Returns:** `indicator_id VARCHAR, indicator_name VARCHAR, country VARCHAR, country_name VARCHAR, year INTEGER, value DOUBLE`

**Supported countries:** SDN, EGY, ETH, TCD, SSD, ERI, LBY, CAF

```sql
-- Sudan population
SELECT * FROM SUDAN_WorldBank('SP.POP.TOTL');

-- Compare Sudan with neighbors
SELECT * FROM SUDAN_WorldBank('SP.POP.TOTL', countries := ['SDN', 'EGY', 'SSD'])
WHERE year >= 2010;

-- GDP per capita
SELECT year, value FROM SUDAN_WorldBank('NY.GDP.PCAP.CD')
WHERE year >= 2000
ORDER BY year;
```

### `SUDAN_WHO(indicator)`
Reads WHO Global Health Observatory data.

**Positional Parameters:**
- `indicator` (VARCHAR, required) — WHO GHO indicator code

**Named Parameters:**
- `countries` (VARCHAR[], optional) — ISO3 country codes. Default: `['SDN']`

**Returns:** `indicator_code VARCHAR, indicator_name VARCHAR, country VARCHAR, year INTEGER, sex VARCHAR, value DOUBLE, region VARCHAR`

```sql
SELECT * FROM SUDAN_WHO('WHOSIS_000001');
```

### `SUDAN_FAO(dataset, element)`
Reads FAO agricultural statistics.

**Positional Parameters:**
- `dataset` (VARCHAR, required) — FAOSTAT dataset code (e.g., 'QCL')
- `element` (VARCHAR, required) — Element name filter (e.g., 'production_quantity')

**Named Parameters:**
- `countries` (VARCHAR[], optional) — ISO3 country codes. Default: `['SDN']`

**Returns:** `dataset VARCHAR, area VARCHAR, item VARCHAR, element VARCHAR, year INTEGER, value DOUBLE, unit VARCHAR`

```sql
SELECT * FROM SUDAN_FAO('QCL', 'production_quantity')
WHERE item = 'Wheat';
```

### `SUDAN_UNHCR(population_type)`
Reads UNHCR displacement and population data.

**Positional Parameters:**
- `population_type` (VARCHAR, required) — One of: 'refugees', 'idps', 'asylum_seekers', 'returned_refugees', 'stateless'

**Named Parameters:**
- `countries` (VARCHAR[], optional) — ISO3 country codes. Default: `['SDN']`

**Returns:** `year INTEGER, population_type VARCHAR, country_origin VARCHAR, country_origin_name VARCHAR, country_asylum VARCHAR, country_asylum_name VARCHAR, value BIGINT`

```sql
SELECT * FROM SUDAN_UNHCR('idps');
SELECT * FROM SUDAN_UNHCR('refugees', countries := ['SDN', 'SSD']);
```

### `SUDAN_ILO(indicator)`
Reads ILO labor statistics.

**Positional Parameters:**
- `indicator` (VARCHAR, required) — ILOSTAT indicator code

**Named Parameters:**
- `countries` (VARCHAR[], optional) — ISO3 country codes. Default: `['SDN']`

**Returns:** `indicator VARCHAR, country VARCHAR, sex VARCHAR, classif1 VARCHAR, year INTEGER, value DOUBLE`

```sql
SELECT * FROM SUDAN_ILO('UNE_DEAP_SEX_AGE_RT');
```

---

## Geospatial Functions

### `SUDAN_Boundaries(level)`
Returns administrative boundaries as GeoJSON strings.

**Positional Parameters:**
- `level` (VARCHAR, required) — 'country', 'state', or 'locality'

**Returns (varies by level):**
- Country: `country_name VARCHAR, country_name_ar VARCHAR, iso_code VARCHAR, geojson VARCHAR`
- State: `state_name VARCHAR, state_name_ar VARCHAR, iso_code VARCHAR, geojson VARCHAR`
- Locality: `locality_name VARCHAR, locality_name_ar VARCHAR, state_name VARCHAR, geojson VARCHAR`

```sql
SELECT state_name, state_name_ar, iso_code, geojson FROM SUDAN_Boundaries('state');
```

### `SUDAN_States()`
Returns all 18 Sudan states with bilingual names, ISO codes, centroids, and GeoJSON.

**Returns:** `state_name VARCHAR, state_name_ar VARCHAR, iso_code VARCHAR, centroid_lon DOUBLE, centroid_lat DOUBLE, geojson VARCHAR`

```sql
SELECT * FROM SUDAN_States();

-- Use with spatial extension
SELECT state_name, ST_GeomFromGeoJSON(geojson) AS geom FROM SUDAN_States();
```

### `SUDAN_GeoCode(state_name)` (Scalar)
Returns the ISO 3166-2 code for a Sudan state name. Accepts Arabic or English input.

**Parameters:**
- `state_name` (VARCHAR) — State name in English or Arabic

**Returns:** VARCHAR (ISO code like 'SD-KH')

```sql
SELECT SUDAN_GeoCode('Khartoum');    -- returns 'SD-KH'
SELECT SUDAN_GeoCode('الخرطوم');     -- returns 'SD-KH'
```
