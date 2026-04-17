# -*- coding: utf-8 -*-
"""
build_sermon_generator.py
-------------------------
Reads  metadata/sermon-weather-passages.csv
Writes sermon-generator.html  (standalone chatbot-style quote finder)

Run from repo root:  python scripts/build_sermon_generator.py
"""

import csv, json

# ── Abbreviation → (api_slug, display_name) ──────────────────────────────────
ABBREV = {
    'Gen':    ('genesis','Gen'),     'Ex':     ('exodus','Ex'),
    'Lev':    ('leviticus','Lev'),   'Num':    ('numbers','Num'),
    'Dtn':    ('deuteronomy','Dtn'), 'Jos':    ('joshua','Jos'),
    'Ri':     ('judges','Judg'),
    '1 Sam':  ('1samuel','1 Sam'),   '2 Sam':  ('2samuel','2 Sam'),
    '1 Kön':  ('1kings','1 Kgs'),    '2 Kön':  ('2kings','2 Kgs'),
    '1. Kön': ('1kings','1 Kgs'),    '2. Kön': ('2kings','2 Kgs'),
    '2. Chr': ('2chronicles','2 Chr'),'1. Chr': ('1chronicles','1 Chr'),
    'Neh':    ('nehemiah','Neh'),
    'Hiob':   ('job','Job'),         'Ps':     ('psalms','Ps'),
    'Spr':    ('proverbs','Prov'),   'Pred':   ('ecclesiastes','Eccl'),
    'Koh':    ('ecclesiastes','Eccl'),
    'Sir':    ('ecclesiasticus','Sir'),'Weish': ('wisdom','Wis'),
    'Jes':    ('isaiah','Isa'),      'Jer':    ('jeremiah','Jer'),
    'Klgl':   ('lamentations','Lam'),'Klag':   ('lamentations','Lam'),
    'Hes':    ('ezekiel','Ezek'),    'Ez':     ('ezekiel','Ezek'),
    'Dan':    ('daniel','Dan'),      'Hos':    ('hosea','Hos'),
    'Joel':   ('joel','Joel'),       'Am':     ('amos','Amos'),
    'Amos':   ('amos','Amos'),
    'Mi':     ('micah','Mic'),       'Mic':    ('micah','Mic'),
    'Nah':    ('nahum','Nah'),       'Hab':    ('habakkuk','Hab'),
    'Zeph':   ('zephaniah','Zeph'),  'Hag':    ('haggai','Hag'),
    'Sach':   ('zechariah','Zech'),  'Mal':    ('malachi','Mal'),
    'Mt':     ('matthew','Matt'),    'Mk':     ('mark','Mark'),
    'Lk':     ('luke','Luke'),       'Joh':    ('john','John'),
    'Apg':    ('acts','Acts'),       'Röm':    ('romans','Rom'),
    '1 Kor':  ('1corinthians','1 Cor'),'2 Kor': ('2corinthians','2 Cor'),
    '1. Kor': ('1corinthians','1 Cor'),
    'Gal':    ('galatians','Gal'),   'Eph':    ('ephesians','Eph'),
    'Phil':   ('philippians','Phil'),'Kol':    ('colossians','Col'),
    '1 Tim':  ('1timothy','1 Tim'),  '2 Tim':  ('2timothy','2 Tim'),
    '1. Tim': ('1timothy','1 Tim'),
    'Hebr':   ('hebrews','Heb'),     'Jak':    ('james','Jas'),
    '1 Petr': ('1peter','1 Pet'),    '2 Petr': ('2peter','2 Pet'),
    'Offb':   ('revelation','Rev'),
}

BASE = ('https://cdn.jsdelivr.net/gh/wldeh/bible-api'
        '/bibles/en-kjv/books/{book}/chapters/{ch}.json')

# ── Build phenomenon → [{loc, url}] ──────────────────────────────────────────
phenom_data = {}
rows = list(csv.DictReader(
    open('metadata/sermon-weather-passages.csv', encoding='utf-8')
))
for row in rows:
    location  = row['CITATION'].strip()
    ph_raw    = row['WEATHER_PHENOMENON'].strip()
    phenomena = [p.strip() for p in ph_raw.split(',') if p.strip()]
    if not phenomena or not location: continue
    parts = location.rsplit(' ', 1)
    if len(parts) != 2: continue
    abbrev, ch = parts[0].strip(), parts[1].strip()
    if abbrev not in ABBREV:
        abbrev = abbrev.rstrip('.')
    if abbrev not in ABBREV:
        continue
    api_slug, display = ABBREV[abbrev]
    entry = {'loc': f'{display} {ch}', 'url': BASE.format(book=api_slug, ch=ch)}
    for ph in phenomena:
        phenom_data.setdefault(ph, [])
        if entry not in phenom_data[ph]:
            phenom_data[ph].append(entry)

phenomena_js  = json.dumps(phenom_data, ensure_ascii=False)
datalist_opts = '\n'.join(f'      <option value="{p}">' for p in sorted(phenom_data))

# ── HTML ──────────────────────────────────────────────────────────────────────
# Note: JS strings use backtick template literals to avoid any quote-escaping
# issues when this Python f-string is rendered. Braces in JS are {{ }}.
HTML = """\
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Little Ice Age — Sermon Quote Generator</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --black:  #111111;
    --dark:   #1a1a1a;
    --panel:  #161616;
    --border: #2a2a2a;
    --red:    #8b1a1a;
    --red-lt: #b52222;
    --grey1:  #888888;
    --grey2:  #555555;
    --text:   #cccccc;
    --white:  #f0f0f0;
  }
  html, body {
    min-height: 100vh; background: var(--black); color: var(--text);
    font-family: Georgia, serif;
  }
  header {
    height: 52px; background: var(--dark); border-bottom: 2px solid var(--red);
    display: flex; align-items: center; padding: 0 24px; gap: 12px;
  }
  header h1 {
    font-size: 1rem; font-weight: normal; letter-spacing: .12em;
    text-transform: uppercase; color: var(--white);
  }
  header .sub { font-size: .8rem; color: var(--grey1); font-style: italic; }

  #wrap {
    max-width: 720px; margin: 0 auto; padding: 2rem 1.5rem;
    display: flex; flex-direction: column; gap: 1.2rem;
  }

  /* ── Bubbles ── */
  .bubble {
    padding: .85rem 1.1rem; border-radius: 1.1rem;
    max-width: 86%; line-height: 1.6; font-size: .95rem;
  }
  .bot {
    background: var(--panel); border: 1px solid var(--border);
    border-bottom-left-radius: .3rem; align-self: flex-start; color: var(--text);
  }
  .user {
    background: var(--red); color: var(--white);
    border-bottom-right-radius: .3rem; align-self: flex-end;
  }

  /* ── Input row ── */
  #input-row { display: flex; gap: .6rem; align-items: center; }
  #q {
    flex: 1; padding: .7rem 1rem;
    background: var(--dark); color: var(--white);
    border: 1px solid var(--border); border-radius: 2rem;
    font-family: Georgia, serif; font-size: .95rem; outline: none;
    transition: border-color .2s;
  }
  #q::placeholder { color: var(--grey2); }
  #q:focus { border-color: var(--red-lt); box-shadow: 0 0 0 2px rgba(139,26,26,.25); }
  #q::-webkit-calendar-picker-indicator { filter: invert(1) opacity(.4); }
  button {
    padding: .7rem 1.4rem; background: var(--red); color: var(--white);
    border: none; border-radius: 2rem; cursor: pointer;
    font-family: Georgia, serif; font-size: .95rem; letter-spacing: .04em;
    transition: background .15s;
  }
  button:hover { background: var(--red-lt); }

  /* ── Cards ── */
  #cards { display: flex; flex-direction: column; gap: .8rem; }
  .card {
    background: var(--panel); border: 1px solid var(--border);
    border-radius: .8rem; padding: 1rem 1.2rem;
    border-left: 3px solid var(--red);
  }
  .card .ref {
    font-family: 'Courier New', monospace; font-size: .78rem;
    color: var(--red-lt); letter-spacing: .08em; text-transform: uppercase;
    margin-bottom: .5rem;
  }
  .card .verse-text { color: var(--text); font-size: .93rem; line-height: 1.6; font-style: italic; }
  .card .verse-text.loading { color: var(--grey2); font-style: normal; }

  /* ── Spinner ── */
  .spinner {
    display: inline-block; width: .85em; height: .85em;
    border: 2px solid var(--border); border-top-color: var(--red-lt);
    border-radius: 50%; animation: spin .7s linear infinite; vertical-align: middle;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<header>
  <h1>Little Ice Age</h1>
  <span class="sub">Sermon Quote Generator</span>
</header>

<div id="wrap">
  <div class="bubble bot">
    Gru&#223; Gott! I see you need to hold a sermon later in the Lukaskirche.<br>
    About which weather phenomenon do you need to talk?
  </div>

  <div id="input-row">
    <input id="q" list="phenomena" placeholder="Hagel, Donner, Wind&#8230;" autocomplete="off">
    <datalist id="phenomena">
DATALIST_PLACEHOLDER
    </datalist>
    <button id="ask-btn">Ask</button>
  </div>

  <div id="results"></div>
</div>

<script>
const DATA = DATA_PLACEHOLDER;

function extractText(json) {
  if (json?.data && Array.isArray(json.data)) return json.data.map(v => v.text || '').join(' ');
  if (Array.isArray(json)) return json.map(v => v.text || '').join(' ');
  const vv = json?.chapter?.verses || json?.verses || [];
  return vv.map(v => v.text || '').join(' ');
}

function ask() {
  const ph  = document.getElementById('q').value.trim();
  const out = document.getElementById('results');
  const entries = DATA[ph];

  if (!entries || !entries.length) {
    out.innerHTML = `<div class="bubble bot">No passages found for <em>${ph}</em>. Try another term.</div>`;
    return;
  }

  out.innerHTML = `
    <div class="bubble user">${ph}</div>
    <div class="bubble bot">Sure! I suggest you use these passages:</div>
    <div id="cards"></div>`;

  const cards = document.getElementById('cards');

  entries.forEach(({loc, url}) => {
    const card = document.createElement('div');
    card.className = 'card';
    card.innerHTML = `<div class="ref">${loc}</div><div class="verse-text loading"><span class="spinner"></span> loading&hellip;</div>`;
    cards.appendChild(card);

    fetch(url)
      .then(r => r.json())
      .then(data => {
        let text = extractText(data);
        if (text.length > 200) text = text.slice(0, 197) + '\u2026';
        const vt = card.querySelector('.verse-text');
        vt.classList.remove('loading');
        vt.textContent = text || '(no text found)';
      })
      .catch(() => {
        const vt = card.querySelector('.verse-text');
        vt.classList.remove('loading');
        vt.textContent = '(could not load passage)';
      });
  });
}

document.getElementById('ask-btn').addEventListener('click', ask);
document.getElementById('q').addEventListener('keydown', e => { if (e.key === 'Enter') ask(); });
</script>
</body>
</html>
"""

# Inject data — done via simple string replace to stay outside the f-string
# and avoid any Python/JS escaping conflicts.
HTML = HTML.replace('DATA_PLACEHOLDER', phenomena_js)
HTML = HTML.replace('DATALIST_PLACEHOLDER', datalist_opts)

with open('sermon-generator.html', 'w', encoding='utf-8') as f:
    f.write(HTML)
print(f'Written sermon-generator.html  ({len(phenom_data)} phenomena, {sum(len(v) for v in phenom_data.values())} passages)')
