# metadata/ — Data Dictionary

This folder contains all source data, manually curated files, and auto-generated outputs used by the `little-ice-age` project. **Auto-generated files should not be edited by hand** — regenerate them by running the corresponding script from the repo root.

All CSV files use comma as delimiter and UTF-8 encoding. Column names follow the pattern UPPER_SNAKE_CASE.

---

## Source data (manual / curated)

### `Predigtensammlung_EcoHack.xlsx`
Master metadata spreadsheet (45 rows, 28 columns). Each row is one early modern source document. Columns include GND numbers, author, normalised title, place of print, coordinates, printer, language, VD number, URL to digitisation, genre, notes, and Bible passage. This is the authoritative source for all sermon metadata. **Script**: `scripts/build_sermons.py` reads this.

### `weather-events.csv`
Columns: `QUERY_CITY`, `DATE`, `LOCATION`, `EVENT_TYPE`
Curated weather event data scraped from tambora.org and manually reviewed. One row per event. `QUERY_CITY` is the city used as the search term on tambora.org; `LOCATION` is the actual reported location. `EVENT_TYPE` uses 23 simplified English categories (e.g. `thunderstorm`, `hail`, `snow`, `flood`). **Script**: `scripts/build_weather_data.py` reads this and writes `weather_data.js`.

### `sermon-weather-passages.csv`
Columns: `SERMON_SLUG`, `CITATION`, `WEATHER_PHENOMENON`
Manually curated table linking each sermon to specific Bible passages cited in the context of weather events, with a German weather phenomenon label. `CITATION` is in abbreviated German form (e.g. `Hiob 38`, `Ex 9`). One row per (sermon, passage, phenomenon) combination. **Script**: `scripts/build_sermon_generator.py` reads this.

### `sermon-dates-corpus.csv`
Columns: `SERMON_SLUG`, `DATE`, `QUOTE`, `DATE_TYPE`
Manually curated table of date expressions found in the sermon texts during a test annotation run. `DATE_TYPE` classifies the date (e.g. `Druckjahr`, `Wetterphanomen`, `Predigtdatum`). Not produced by any automated script. For research reference only.

### `citations-network-sermons.gephi`
Gephi project file for the sermon-Bible citation network. Opened and saved manually in Gephi. Complement to `network-sermon-bible-edges.csv`.

---

## Auto-generated outputs

### `sermon-bible-references.csv`
Columns: `SERMON_SLUG`, `CITATION_RAW`, `CITATION`, `BOOK_SLUG`, `CHAPTER`, `VERSE`, `API_URL`
One row per Bible reference automatically extracted from the sermon texts. `CITATION_RAW` is the original Fruhneuhochdeutsch form (e.g. `Hiob am 38`); `CITATION` is the standardised English form (e.g. `Job 38`); `BOOK_SLUG` is the API identifier (e.g. `job`); `API_URL` links to the wldeh/bible-api CDN. **Regenerate**: `python scripts/extract_bible_citations.py`

### `sermon-weather-phenomena.csv`
Columns: `SERMON_SLUG`, `JSON_FILE`, `WEATHER_PHENOMENON`
One row per (sermon x weather phenomenon) pair, produced by matching German weather terms against the full sermon text. **Regenerate**: `python scripts/build_phenomena.py`

### `network-sermon-bible-edges.csv`
Columns: `Source`, `Target`
Bipartite edge list for network analysis. `Source` is a sermon slug; `Target` is a standardised Bible citation (e.g. `Job 38`). Import into Gephi via Data Laboratory -> Import Spreadsheet -> Edge table. **Regenerate**: `python scripts/extract_bible_citations.py` (produced alongside `sermon-bible-references.csv`).

### `tambora_scrape.csv` *(when present)*
Columns: `QUERY_CITY`, `DATE`, `LOCATION`, `EVENT_TYPE`
Raw output of the Tambora.org web scraper. Must be manually inspected and merged into `weather-events.csv` before use in the pipeline. **Regenerate**: `python scripts/tambora_scrape.py` (requires internet access).

---

## Manually curated folders

### `bible-passages/` (8 CSV files)
Full passage text for selected Bible citations, one CSV per sermon. Compiled manually for close-reading research. Not produced by any script.

---

## Pipeline overview

```
Predigtensammlung_EcoHack.xlsx + txt/*.txt
    -> scripts/build_sermons.py               -> json/*.json  +  data.js

json/*.json
    -> scripts/build_phenomena.py             -> sermon-weather-phenomena.csv
                                                 phenomena_data.js
    -> scripts/extract_bible_citations.py     -> sermon-bible-references.csv
                                                 network-sermon-bible-edges.csv

sermon-weather-passages.csv (manual)
    -> scripts/build_sermon_generator.py      -> sermon-generator.html

weather-events.csv (manual)
    -> scripts/build_weather_data.py          -> weather_data.js

[tambora.org]
    -> scripts/tambora_scrape.py              -> tambora_scrape.csv  (then manually merged)
```
