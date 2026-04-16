"""
extract_weather_phenomena.py

Searches for weather-related terms in the "text" field of JSON files
and outputs a CSV with columns: FILENAME, PHENOMENON.

Usage:
    python extract_weather_phenomena.py <input_path> [--output output.csv]

    <input_path> can be:
        - a single .json file
        - a directory (all .json files inside will be processed)

    --output: optional output CSV path (default: results.csv)
"""

import json
import csv
import re
import os
import argparse
from pathlib import Path

# ── Weather terms ────────────────────────────────────────────────────────────
# Each entry is a canonical label plus all spelling variants to search for.
# Search is case-insensitive and matches whole tokens (word boundaries).

WEATHER_TERMS = {
    "Wetter":           ["Wetter"],
    "Witterung/Gewitter": ["Witterung", "Gewitter", "Witter"],
    "Meteor/Meteorologie": ["Meteor", "Meteorologie"],
    "Wind":             ["Wind"],
    "Finsternuß/Finsternis": ["Finsternuß", "Finsternis"],
    "Winter":           ["Winter", "Wintr"],
    "Regen/Blutregen/Regenbogen": ["Regen", "Blutregen", "Regenbogen", "Regn"],
    "Nebel":            ["Nebel"],
    "Komet/Comet":      ["Komet", "Comet"],
    "Frost":            ["Frost"],
    "Kälte":            ["Kaelt", "Kält", "Kaelte", "Kälte"],
    "Schnee":           ["Schnee"],
    "Eis":              ["Eis"],
    "Dürre":            ["Dürre", "Durre"],
    "Trockenheit":      ["Trock", "Trocknung", "Trockenheit"],
    "Hagel":            ["Hagel"],
    "Hitze":            ["Hitze"],
    "Sturm":            ["Sturm"],
    "Feuchte":          ["Feuchte"],
    "Niederschlag":     ["Niederschlag"],
    "Dampf":            ["Dampf"],
    "Exhalationes":     ["Exhalationes"],
    "Dunkst":           ["Dunkst"],
    "Vapor":            ["Vapor"],
    "Flut":             ["Flut"],
    "Sommer":           ["Sommer"],
    "Herbst":           ["Herbst"],
    "Frühling/Frühjahr/Lenz": ["Frühling", "Fruehling", "Frühjahr", "Fruehjahr", "Lenz"],
    "kalt":                 ["kalt"],
    "heiß":                 ["heiß", "heiss"],
    "Schlossen/Schloßen":   ["Schlossen", "Schloßen"],
    "Wasser":               ["Wasser"],
    "Sonne":                ["Sonne"],
    "Verdunkelung/Dunkelheit": ["Verdunkelung", "Dunkelheit"],
    "Wolke":                ["Wolke"],
}

# Pre-compile one regex per canonical term (all variants OR-joined)
COMPILED_TERMS = {
    label: re.compile(
        r'\b(' + '|'.join(re.escape(v) for v in variants) + r')\b',
        re.IGNORECASE
    )
    for label, variants in WEATHER_TERMS.items()
}


def extract_text(record):
    """Return the value of the 'text' field from a JSON record (dict or list)."""
    if isinstance(record, dict):
        return record.get("text", "")
    return ""


def find_phenomena(text):
    """Return a sorted list of canonical labels found in text."""
    found = []
    for label, pattern in COMPILED_TERMS.items():
        if pattern.search(text):
            found.append(label)
    return found


def process_file(json_path):
    """
    Load a JSON file and return a list of (filename, phenomenon) tuples.
    Handles both a single record (dict) and a list of records.
    """
    rows = []
    filename = Path(json_path).name

    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[WARN] Could not read {json_path}: {e}")
        return rows

    # Normalise to list
    records = data if isinstance(data, list) else [data]

    for record in records:
        text = extract_text(record)
        if not text:
            continue
        for phenomenon in find_phenomena(text):
            rows.append((filename, phenomenon))

    return rows


def collect_json_files(input_path):
    """Return all .json files under input_path (file or directory)."""
    p = Path(input_path)
    if p.is_file():
        return [p]
    elif p.is_dir():
        return sorted(p.rglob("*.json"))
    else:
        raise FileNotFoundError(f"Input path not found: {input_path}")


def main():
    parser = argparse.ArgumentParser(description="Extract weather phenomena from JSON files.")
    parser.add_argument("input_path", help="Path to a .json file or a directory of .json files")
    parser.add_argument("--output", default="results.csv", help="Output CSV file (default: results.csv)")
    args = parser.parse_args()

    json_files = collect_json_files(args.input_path)
    print(f"Found {len(json_files)} JSON file(s).")

    all_rows = []
    for jf in json_files:
        rows = process_file(jf)
        all_rows.extend(rows)
        print(f"  {jf.name}: {len(rows)} match(es)")

    with open(args.output, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["FILENAME", "PHENOMENON"])
        writer.writerows(all_rows)

    print(f"\nDone. {len(all_rows)} row(s) written to '{args.output}'.")


if __name__ == "__main__":
    main()
