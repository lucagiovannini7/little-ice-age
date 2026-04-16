# -*- coding: utf-8 -*-
import pandas as pd
import json
import os
import unicodedata

df = pd.read_excel('metadata/Predigtensammlung_EcoHack.xlsx', sheet_name='Tabelle1')

mapping = {
    '1556_Rörer_Ein_Predig_von_dem_Leüten_gegen_das_wetter': 0,
    '1565_Alber_Ein_Summa_etlicher_Predigen_von_Hagel_und_Unholden_gethon_in_der_Pfarkirch_zuo_S': 1,
    '1566_Rorer_Eine_Predigt_von_dem_Leuten_gegen_das_Wetter': 2,
    '1571_Lavater_Von_Thüwre_und_Hunger_dry_Predigen': 3,
    '1589_Alberum_Bidenbach_Ein_Summa_etlicher_Predigen_vom_Hagel_un': 5,
    '1599_Fabricius_Christliche_Predigt_von_den_grossen_vnd_kleinen_Bäumen': 6,
    '1613_Conciones_de_tempore_3__Predigt_Schnee': 8,
    '1616_Titius_Predigt_Vom_Donner_Hagel_und_Blitzen': 9,
    '1658_Tappe_Der_Mensch_weiß_seine_Zeit_nicht__LeichPredigt': 44,
    '1664_Heland_Est_Visa_Dei_Gloria,_In_Fulgurita_Curia': 22,
    '1666_Barthold_Des_Himmelskönigs_Wetter-Geschütze': 23,
    '1670_Eigentliche_und_warhaffte_Relation': 27,
    '1673_Eigentliche_Beschreibung_deren_im_Monat_Nov__1672': 28,
    '1706_Cuno_Den_Schnee': 39,
    'trans_1637_Schilling_Drey_Christliche_Thewrung': 17,
    'trans_1639_Reinhold_Bronteiologiké': 19,
    'trans_1649_Alard_Hiobitsche_Trübsaell': 20,
    'trans_1705_Andreae_Gottes_Herrliche,_Heilige_und_Heilsame_Wege_im_Wetter': 36,
    'trans_1705_Gräfe_Gottes_Majestät_und_Herrligkeit_bey_dem_Ungewöhnlichen_grossen_Schnee': 37,
    'trans_1707_Götze_El_Iemosha_ot_Der_Gott_vieler_Hülffen': 38,
    'trans_1748_Knebel_Wie_Gott_denen_Leuten_die_Ohren_zu_öffnen_pflege': 43,
}

os.makedirs('json', exist_ok=True)

# Build NFC-normalized name → actual filename map to handle NFD filenames on disk
txt_files = {unicodedata.normalize('NFC', f): f for f in os.listdir('txt') if f.endswith('.txt')}

produced = []
for basename, row_idx in mapping.items():
    nfc_key = unicodedata.normalize('NFC', basename) + '.txt'
    actual_filename = txt_files.get(nfc_key)
    if not actual_filename:
        print(f'WARNING: file not found for {basename}')
        continue
    txt_path = f'txt/{actual_filename}'
    json_path = f'json/{basename}.json'

    text = open(txt_path, encoding='utf-8').read()

    row = df.iloc[row_idx]
    data = {}
    for col in df.columns:
        val = row[col]
        if pd.isna(val):
            data[col] = None
        elif hasattr(val, 'item'):
            data[col] = val.item()
        else:
            data[col] = val
    data['text'] = text

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    produced.append(basename)

print(f'Produced {len(produced)} JSON files.')
for p in produced:
    print(f'  {p}.json')
