# -*- coding: utf-8 -*-
"""
build_sermons.py
----------------
STAGE 1 of the data pipeline.
Reads  metadata/Predigtensammlung_EcoHack.xlsx  +  txt/*.txt
Writes json/*.json  (one per sermon, with slug + all metadata + full text)
Writes data.js      (const SERMONS = [...], loaded by sermon_timeline.html)

Run from repo root:  python scripts/build_sermons.py
"""
import pandas as pd, json, re, os, unicodedata, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

ENTRIES = [
    ('1556_Rörer_Ein_Predig_von_dem_Leüten_gegen_das_wetter', 0),
    ('1565_Alber_Ein_Summa_etlicher_Predigen_von_Hagel_und_Unholden_gethon_in_der_Pfarkirch_zuo_S', 1),
    ('1566_Rorer_Eine_Predigt_von_dem_Leuten_gegen_das_Wetter', 2),
    ('1571_Lavater_Von_Thüwre_und_Hunger_dry_Predigen', 3),
    ('1589_Alberum_Bidenbach_Ein_Summa_etlicher_Predigen_vom_Hagel_un', 5),
    ('1599_Fabricius_Christliche_Predigt_von_den_grossen_vnd_kleinen_Bäumen', 6),
    ('1613_Conciones_de_tempore_3__Predigt_Schnee', 8),
    ('1616_Titius_Predigt_Vom_Donner_Hagel_und_Blitzen', 9),
    ('1658_Tappe_Der_Mensch_weiß_seine_Zeit_nicht__LeichPredigt', 44),
    ('1664_Heland_Est_Visa_Dei_Gloria,_In_Fulgurita_Curia', 22),
    ('1666_Barthold_Des_Himmelskönigs_Wetter-Geschütze', 23),
    ('1706_Cuno_Den_Schnee', 39),
    ('new_trans_1720_Tafinger_Der_Ausbruch_göttlicher_Gerichte_in_dem_Tübingischen_fast_unerhörten_Wetter-Schaden', 40),
    ('new_trans_1720_Pregitzer_Das_von_Gott_zwar_hart_gestraffte_und_gezüchtigte_[___]_Tübingen', 41),
    ('trans_1637_Schilling_Drey_Christliche_Thewrung', 17),
    ('trans_1639_Reinhold_Bronteiologiké', 19),
    ('trans_1649_Alard_Hiobitsche_Trübsaell', 20),
    ('trans_1705_Andreae_Gottes_Herrliche,_Heilige_und_Heilsame_Wege_im_Wetter', 36),
    ('trans_1705_Gräfe_Gottes_Majestät_und_Herrligkeit_bey_dem_Ungewöhnlichen_grossen_Schnee', 37),
    ('trans_1707_Götze_El_Iemosha_ot_Der_Gott_vieler_Hülffen', 38),
    ('trans_1748_Knebel_Wie_Gott_denen_Leuten_die_Ohren_zu_öffnen_pflege', 43),
]

REMOVE = {
    'Unnamed: 17', 'bearbeitet von', 'korrigiert von',
    'Personen in separater Tabelle angelegt?', 'Text transkribiert?',
    'Titelblatt in Transkribus korrigiert?', 'Ziffern in Transkribus korrigiert?',
    'Gesamter Text korrigiert?', 'zusammengefasst?', 'Unnamed: 27',
}
RENAME = {
    'GND-Nummer (Autor)': 'gnd_author',
    'Jahr': 'year',
    'Autor': 'author',
    'Titel Original': 'title_original',
    'Titel normalisiert nach heutigem Sprachgebrauch (nur Orthographie, vorerst nicht ausfüllen)': 'title_normalized',
    'Druckort': 'place_of_print',
    'Druckort (Pseudonym)': 'place_of_print_pseudonym',
    'Koordinaten Druckort': 'coordinates',
    'GND-Nummer (Drucker)': 'gnd_printer',
    'Drucker': 'printer',
    'weitere Personen': 'other_persons',
    'GND-Nummer (weitere Personen)': 'gnd_other_persons',
    'Sprache': 'language',
    'VD-Nummer': 'vd_number',
    'URL zum Digitalisat': 'url_digitization',
    'Gattung': 'genre',
    'Notizen': 'notes',
    'Bibelstelle': 'bible_passage',
}

PRIME = '\u2032'
UMLAUT = str.maketrans({'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'Ä': 'ae', 'Ö': 'oe', 'Ü': 'ue', 'ß': 'ss'})

def slugify(s):
    s = s.translate(UMLAUT)
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    s = re.sub(r'[^a-z0-9]+', ' ', s.lower()).strip()
    return s.replace(' ', '-') if s else ''

def make_slug(year_raw, author_raw, title_raw):
    year = str(year_raw or '').strip('[] ')
    surname = re.split(r'[,;]', str(author_raw or ''))[0].strip()
    words = []
    for w in str(title_raw or '').split():
        s = slugify(w)
        if s:
            words.append(s)
        if len(words) == 3:
            break
    return '-'.join([year, slugify(surname)] + words)

def normalize_coord(raw):
    if not raw or str(raw) == 'nan':
        return None
    s = str(raw).strip()
    # decimal degrees e.g. 47.56° N, 7.59° E
    m = re.match(r'(\d+\.\d+)[°]\s*N,?\s*(\d+\.\d+)[°]\s*[EeOo]', s)
    if m:
        lat_d, lon_d = float(m.group(1)), float(m.group(2))
        return (f'{int(lat_d)}{chr(176)} {round((lat_d - int(lat_d)) * 60)}{PRIME} N, '
                f'{int(lon_d)}{chr(176)} {round((lon_d - int(lon_d)) * 60)}{PRIME} E')
    # DMS with seconds - various apostrophe chars
    min_chars = "[\u2018\u2019\u2032']"
    pat = (r'(\d+)[°]\s*(\d+)' + min_chars + r'\s*([\d.]+)\s*N'
           r'\s+(\d+)[°]\s*(\d+)' + min_chars + r'\s*([\d.]+)\s*[EeOo]')
    m = re.search(pat, s)
    if m:
        lat_min = round(int(m.group(2)) + float(m.group(3)) / 60)
        lon_min = round(int(m.group(5)) + float(m.group(6)) / 60)
        return (f'{m.group(1)}{chr(176)} {lat_min}{PRIME} N, '
                f'{m.group(4)}{chr(176)} {lon_min}{PRIME} E')
    # standard DMS - normalize O->E, unify primes, spacing
    s = re.sub(r'\bO\b', 'E', s)
    s = s.replace('\u2019', PRIME).replace("'", PRIME).replace('\u2018', PRIME)
    s = re.sub(r'\s*,\s*', ', ', s)
    s = re.sub(r' {2,}', ' ', s)
    return s.strip()


df = pd.read_excel('metadata/Predigtensammlung_EcoHack.xlsx', sheet_name='Tabelle1')
txt_map = {unicodedata.normalize('NFC', f): f for f in os.listdir('txt') if f.endswith('.txt')}

all_data = []
for basename, row_idx in ENTRIES:
    nfc_key = unicodedata.normalize('NFC', basename) + '.txt'
    actual = txt_map.get(nfc_key)
    if not actual:
        print(f'TXT MISSING: {nfc_key}')
        continue
    text = open(f'txt/{actual}', encoding='utf-8').read()
    row = df.iloc[row_idx]

    data = {'slug': make_slug(row['Jahr'], row['Autor'], row['Titel Original'])}
    for old_key, new_key in RENAME.items():
        if old_key in REMOVE:
            continue
        val = row[old_key] if old_key in row.index else None
        if val is None or str(val) == 'nan':
            data[new_key] = None
        elif hasattr(val, 'item'):
            data[new_key] = val.item()
        else:
            data[new_key] = val
    data['coordinates'] = normalize_coord(row['Koordinaten Druckort'])
    data['text'] = text

    json_path = f'json/{basename}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    all_data.append(data)
    print(f'{data["slug"]}  coords={data["coordinates"]}')

with open('data.js', 'w', encoding='utf-8') as f:
    f.write('const SERMONS = ')
    json.dump(all_data, f, ensure_ascii=False, indent=2)
    f.write(';\n')
print(f'\nDone: {len(all_data)} JSONs + data.js')
