"""
Generate sudan_boundaries_data.hpp from GADM GeoJSON.
Simplifies coordinates to 3 decimal places and generates C++ string literals.
Handles MSVC's 16380-char string literal limit by splitting long strings.
"""
import json
import sys
import os

# MSVC string literal limit
MAX_LITERAL_LEN = 15000  # Leave margin below 16380

# Map GADM NAME_1 -> our ISO code
GADM_TO_ISO = {
    "AlJazirah":       "SD-GZ",
    "AlQadarif":       "SD-GD",
    "BlueNile":        "SD-NB",
    "CentralDarfur":   "SD-DC",
    "EastDarfur":      "SD-DE",
    "Kassala":         "SD-KA",
    "Khartoum":        "SD-KH",
    "NorthDarfur":     "SD-DN",
    "NorthKurdufan":   "SD-KN",
    "Northern":        "SD-NO",
    "RedSea":          "SD-RS",
    "RiverNile":       "SD-NR",
    "Sennar":          "SD-SI",
    "SouthDarfur":     "SD-DS",
    "SouthKurdufan":   "SD-KS",
    "WestDarfur":      "SD-DW",
    "WestKurdufan":    "SD-KW",
    "WhiteNile":       "SD-NW",
}

def simplify_coords(coords, precision=3):
    """Recursively round coordinates to given precision."""
    if isinstance(coords, float):
        return round(coords, precision)
    if isinstance(coords, int):
        return float(coords)
    if isinstance(coords, list):
        return [simplify_coords(c, precision) for c in coords]
    return coords

def remove_duplicate_consecutive(ring):
    """Remove consecutive duplicate points in a ring."""
    if len(ring) <= 1:
        return ring
    result = [ring[0]]
    for i in range(1, len(ring)):
        if ring[i] != ring[i-1]:
            result.append(ring[i])
    # Ensure ring is closed
    if result[0] != result[-1]:
        result.append(result[0])
    return result

def simplify_geometry(geometry, precision=3):
    """Simplify a MultiPolygon geometry."""
    coords = simplify_coords(geometry["coordinates"], precision)

    # Remove consecutive duplicate points after rounding
    new_coords = []
    for polygon in coords:
        new_polygon = []
        for ring in polygon:
            cleaned = remove_duplicate_consecutive(ring)
            if len(cleaned) >= 4:  # Valid ring needs at least 4 points
                new_polygon.append(cleaned)
        if new_polygon:
            new_coords.append(new_polygon)

    return {"type": geometry["type"], "coordinates": new_coords}

def format_json_compact(obj):
    """Format JSON compactly with no spaces."""
    return json.dumps(obj, separators=(',', ':'))


def make_cpp_string_literal(s):
    """
    Create a C++ string expression from a potentially long string.
    For strings under MAX_LITERAL_LEN, returns a single raw string literal.
    For longer strings, returns a std::string concatenation expression.
    """
    if len(s) <= MAX_LITERAL_LEN:
        return f'R"geojson({s})geojson"'

    # Split into chunks at safe boundaries (between coordinate arrays)
    chunks = []
    while len(s) > MAX_LITERAL_LEN:
        # Find a good split point - look for ],[ pattern near the limit
        split_at = MAX_LITERAL_LEN
        # Search backwards for a comma that separates coordinate groups
        for i in range(split_at, split_at - 2000, -1):
            if s[i] == ',' and s[i-1] == ']':
                split_at = i + 1  # Include the comma
                break
        chunks.append(s[:split_at])
        s = s[split_at:]
    chunks.append(s)
    return chunks


def main():
    input_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gadm41_SDN_1.json")
    output_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                               "src", "sudan", "geo", "sudan_boundaries_data.hpp")

    print(f"Reading {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Process features
    state_geojsons = {}
    all_coords_for_country = []

    for feat in data["features"]:
        name = feat["properties"]["NAME_1"]
        if name not in GADM_TO_ISO:
            print(f"WARNING: Unknown state name '{name}', skipping")
            continue

        iso = GADM_TO_ISO[name]
        simplified = simplify_geometry(feat["geometry"], precision=3)
        geojson_str = format_json_compact(simplified)
        state_geojsons[iso] = geojson_str

        # Collect for country boundary
        all_coords_for_country.extend(simplified["coordinates"])

        print(f"  {name:25s} -> {iso}  {len(geojson_str):>6d} chars")

    # Create country boundary
    country_geojson = format_json_compact({
        "type": "MultiPolygon",
        "coordinates": all_coords_for_country
    })
    print(f"  {'Country (all merged)':25s}          {len(country_geojson):>6d} chars")

    # State order matching SUDAN_STATES[] array
    iso_order = [
        "SD-KH", "SD-GZ", "SD-GD", "SD-KA", "SD-RS", "SD-NR", "SD-NO", "SD-NW",
        "SD-NB", "SD-SI", "SD-DS", "SD-DN", "SD-DW", "SD-DC", "SD-DE", "SD-KN",
        "SD-KS", "SD-KW"
    ]

    # Generate C++ header
    total_size = 0
    lines = []
    lines.append("#pragma once")
    lines.append("")
    lines.append("#include <string>")
    lines.append("")
    lines.append("// Auto-generated from GADM v4.1 (gadm41_SDN_1.json)")
    lines.append("// Coordinates simplified to 3 decimal places (~111m accuracy)")
    lines.append("// Do not edit manually - regenerate with scripts/generate_boundaries.py")
    lines.append("")
    lines.append("namespace duckdb {")
    lines.append("")
    lines.append("namespace {")
    lines.append("")

    # Helper function to get boundary as std::string (handles concatenation for long strings)
    # For short strings, use const char* directly
    # For long strings, use static std::string initialized from concatenated literals

    # First, emit individual state variables for long strings
    long_state_vars = {}
    for idx, iso in enumerate(iso_order):
        geojson = state_geojsons[iso]
        total_size += len(geojson)
        result = make_cpp_string_literal(geojson)
        if isinstance(result, list):
            # Long string - needs a variable with concatenated literals
            var_name = f"BOUNDARY_{iso.replace('-', '_')}"
            long_state_vars[iso] = var_name
            lines.append(f"// {iso} ({len(geojson)} chars, split into {len(result)} chunks)")
            concat_parts = []
            for i, chunk in enumerate(result):
                concat_parts.append(f'    R"geojson({chunk})geojson"')
            lines.append(f"static const std::string {var_name} =")
            lines.append("\n".join(concat_parts) + ";")
            lines.append("")

    # State boundary array
    lines.append("// GeoJSON polygon boundaries for each state, indexed same as SUDAN_STATES[]")
    lines.append("// Short strings are const char*, long strings reference std::string variables above")
    lines.append("")

    # Since some are std::string and some are const char*, we'll use a function to return std::string
    lines.append("static std::string GetStateBoundaryGeoJSON(idx_t index) {")
    lines.append("    switch (index) {")
    for idx, iso in enumerate(iso_order):
        geojson = state_geojsons[iso]
        if iso in long_state_vars:
            lines.append(f"    case {idx}: return {long_state_vars[iso]};")
        else:
            literal = make_cpp_string_literal(geojson)
            lines.append(f"    case {idx}: return {literal};")
    lines.append('    default: return "";')
    lines.append("    }")
    lines.append("}")
    lines.append("")

    # Country boundary - also potentially long
    total_size += len(country_geojson)
    country_result = make_cpp_string_literal(country_geojson)
    lines.append("// Country boundary (union of all state polygons)")
    if isinstance(country_result, list):
        concat_parts = []
        for chunk in country_result:
            concat_parts.append(f'    R"geojson({chunk})geojson"')
        lines.append(f"static const std::string COUNTRY_BOUNDARY_GEOJSON =")
        lines.append("\n".join(concat_parts) + ";")
    else:
        lines.append(f"static const std::string COUNTRY_BOUNDARY_GEOJSON = {country_result};")
    lines.append("")
    lines.append("} // namespace")
    lines.append("")
    lines.append("} // namespace duckdb")
    lines.append("")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"\nGenerated {output_file}")
    print(f"Total GeoJSON size: {total_size:,d} chars (~{total_size/1024:.0f} KB)")


if __name__ == "__main__":
    main()
