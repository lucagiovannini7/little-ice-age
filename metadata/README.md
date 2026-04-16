# metadata/ — Data Dictionary

This folder contains all source data, manually curated files, and auto-generated outputs used by the `little-ice-age` project. **Auto-generated files should not be edited by hand** — regenerate them by running the corresponding script from the repo root.

---

## Source data (manual / curated)

### `Predigtensammlung_EcoHack.xlsx`
Master metadata spreadsheet (45 rows, 28 columns). Each row is one early modern source document. Columns include GND numbers, author, normalised title, place of print, coordinates, printer, language, VD number, URL to digitisation, genre, notes, and Bible passage. This is the authoritative source for all sermon metadata. **Script**: `scripts/build_sermons.py` reads this.

### `weather-events.csv`
Curated weather event data scraped from [tambora.org](https://www.tambora.org) and manually reviewed. Columns: `CITY`, `PERIOD`, `POSITION`, `CODING`. One row per event. The `CODING` column uses 23 simplified English categories (e.g. `thunderstorm`, `hail`, `snow`, `flood`). **Script**: `scripts/build_weather_data.py` reads this and writes `weather_data.js`.

### `dates_found_in_test_corpus.csv`
Manually curated table of date expressions found in the sermon texts during a test annotation run. Not produced by any automated script. For research reference only.

### `citations-network-sermons.gephi`
Gephi project file for the sermon-Bible citation network. Opened and saved manually in Gephi. Complement to `gephi-edges.csv`.

---

## Auto-generated outputs

### `sermon-phenomena.csv`
One row per (sermon x weather phenomenon) pair. Columns: `SLUG`, `FILENAME`, `PHENOMENON`. Produced by `scripts/build_phenomena.py`, which scans the `text` field of every JSON in `json/` and matches against the `WEATHER_TERMS` dictionary. **Regenerate**: `python scripts/build_phenomena.py`

### `bible-citations.csv`
One row per Bible reference extracted from the sermon texts. Columns: `SLUG`, `BIBLE CITATION` (raw Fruhneuhochdeutsch form), `STANDARDISED CITATION` (e.g. `Job 38`), `BOOK_EN` (API slug), `CHAPTER`, `VERSE`, `URL` (link to wldeh/bible-api). **Regenerate**: `python scripts/extract_bible_citations.py`

### `gephi-edges.csv`
Bipartite edge list for Gephi. Columns: `Source` (sermon slug), `Target` (standardised Bible citation, e.g. `Job 38`). Import into Gephi via Data Laboratory -> Import Spreadsheet -> Edge table. **Regenerate**: `python scripts/extract_bible_citations.py` (produced alongside `bible-citations.csv`).

### `tambora_scrape.csv`
Raw output of the Tambora.org scraper. Columns: `CITY`, `PERIOD`, `POSITION`, `CODING`. This file is the **unreviewed** scraper output -- it must be manually inspected and merged into `weather-events.csv` before use in the pipeline. **Regenerate**: `python scripts/tambora_scrape.py` (requires internet access and `requests`/`beautifulsoup4`).

---

## Manually curated folders

### `bible-passages/` (8 CSV files)
Full passage text for selected Bible citations, one CSV per sermon. Compiled manually for close-reading research. Not produced by any script and not loaded by `sermon_timeline.html`.

---

## Pipeline overview

```
Predigtensammlung_EcoHack.xlsx + txt/*.txt
    -> scripts/build_sermons.py           -> json/*.json  +  data.js

json/*.json
    -> scripts/build_phenomena.py         -> sermon-phenomena.csv  +  phenomena_data.js
    -> scripts/extract_bible_citations.py -> bible-citations.csv   +  gephi-edges.csv

weather-events.csv
    -> scripts/build_weather_data.py      -> weather_data.js

[tambora.org]
    -> scripts/tambora_scrape.py          -> tambora_scrape.csv  (then manually merged)
```
