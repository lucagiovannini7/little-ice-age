# -*- coding: utf-8 -*-
"""
extract_bible_citations.py
--------------------------
Extracts Bible references from the 'text' field of every JSON in json/.
Handles Early New High German (Frühneuhochdeutsch) orthographic variants,
including Roman-numeral chapter numbers.

Output columns:
  SLUG | BIBLE CITATION | BOOK_EN | CHAPTER | VERSE | URL

BOOK_EN  – slug usable in the wldeh/bible-api, e.g. 'john', '1kings'
CHAPTER  – Arabic chapter number (Roman numerals converted)
VERSE    – starting verse if explicitly present in the citation, else blank
URL      – https://cdn.jsdelivr.net/gh/wldeh/bible-api/bibles/en-kjv/
              books/{BOOK_EN}/chapters/{CHAPTER}/verses/{VERSE or 1}.json
"""

import re, json, csv
from pathlib import Path

JSON_DIR   = 'json'
OUTPUT_CSV = 'bible-citations.csv'
API_BASE   = ('https://cdn.jsdelivr.net/gh/wldeh/bible-api'
              '/bibles/en-kjv/books/{book}/chapters/{ch}/verses/{v}.json')

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Book table   (regex, canonical_label, en_slug_for_api)
#     • regex     – matches the book name/abbrev in Frühneuhochdeutsch
#     • label     – internal canonical key used in parse_citation()
#     • en_slug   – exact directory name in wldeh/bible-api (no hyphens)
#                   None = apocryphal / ambiguous, resolved at parse time
# ─────────────────────────────────────────────────────────────────────────────
BOOKS = [
    # ── Pentateuch / Mose ────────────────────────────────────────────────────
    (r'Genesis|Gen',                                        'Gen',      'genesis'),
    (r'Exod(?:iam?|ium?|i)?|Exodus',                       'Ex',       'exodus'),
    (r'Levit(?:icus)?|Lev',                                'Lev',      'leviticus'),
    (r'Numer(?:i(?:ca)?)?|Num',                            'Num',      'numbers'),
    (r'Deuteronom(?:ium|ii)?|Deut|Dtn',                   'Dtn',      'deuteronomy'),
    # "2. Buch Mose" / "5. Mose" etc. — numbered variant BEFORE bare Mose
    (r'[12345]\.?\s*(?:Buch\s+)?Mose?s?',                 'MoseN',    None),   # resolved below
    (r'Mose?s?',                                           'Mose',     None),   # resolved below
    # ── Historical ───────────────────────────────────────────────────────────
    (r'Josu[ae]?|Jos',                                     'Jos',      'joshua'),
    (r'Richt(?:er)?|Judic(?:um)?',                        'Ri',       'judges'),
    (r'Ruth?',                                             'Rut',      'ruth'),
    (r'2\.?\s*(?:Buch\s+)?Sam(?:uel)?|2\.?\s*Saimmel',   '2Sam',     '2samuel'),
    (r'1\.?\s*(?:Buch\s+)?Sam(?:uel)?|1\.?\s*Saimmel',   '1Sam',     '1samuel'),
    (r'Sam(?:uel)?',                                       'Sam',      '1samuel'),  # ambiguous → default 1
    (r'2\.?\s*(?:Buch\s+)?(?:K(?:ö|oe)n(?:ig(?:e(?:n)?)?)?|Reg)', '2Koen', '2kings'),
    (r'1\.?\s*(?:Buch\s+)?(?:K(?:ö|oe)n(?:ig(?:e(?:n)?)?)?|Reg)', '1Koen', '1kings'),
    (r'K(?:ö|oe)n(?:ig(?:e(?:n)?)?)?|Reg',               'Koen',     '1kings'),  # ambiguous → default 1
    (r'2\.?\s*(?:Buch\s+)?Chron(?:ica|ik)?|2\.?\s*Chr',  '2Chr',     '2chronicles'),
    (r'1\.?\s*(?:Buch\s+)?Chron(?:ica|ik)?|1\.?\s*Chr',  '1Chr',     '1chronicles'),
    (r'Esra|Esr',                                          'Esr',      'ezra'),
    (r'Nehemia|Neh',                                       'Neh',      'nehemiah'),
    (r'Esther?|Est',                                       'Est',      'esther'),
    # ── Wisdom ───────────────────────────────────────────────────────────────
    (r'H(?:i|ie)ob(?:o|s)?|Hiobo|Job',                   'Hiob',     'job'),
    (r'Psal(?:m(?:en|o|us)?|ter)?|Pfalm|Psal',           'Ps',       'psalms'),
    (r'Spr(?:ü|ue)ch(?:e|en|lein)?|Sprichw|Proverb(?:ien|ia)?|Prov', 'Spr', 'proverbs'),
    (r'Pred(?:iger(?:salomonis)?)?|Eccl(?:es(?:iastes)?)?|Qoh', 'Pred', 'ecclesiastes'),
    (r'Hohe(?:s?\s+)?Lied|Cant(?:ica|icum)?|Hld',        'Hld',      'songofsolomon'),
    (r'Weish(?:eit)?|Sap(?:ient)?',                       'Weish',    'wisdom'),
    (r'Sir(?:ach)?|Eccli',                                 'Sir',      'ecclesiasticus'),
    # ── Major Prophets ───────────────────────────────────────────────────────
    (r'[GJE]es(?:a|e)ia(?:s)?|Isai(?:as?)?|Jes|Esa(?:i)?', 'Jes',   'isaiah'),
    (r'[VDH]?(?:Jeremia|Hieremia|Hierem|Jerem)|Veremia|Deremia|Jer', 'Jer', 'jeremiah'),
    (r'Klag(?:lieder)?|Thren(?:i)?',                      'Klgl',     'lamentations'),
    (r'H(?:e|E)s(?:ekiel)?|Ez(?:ech(?:iel)?)?',          'Hes',      'ezekiel'),
    (r'Daniel|Dan',                                        'Dan',      'daniel'),
    # ── Minor Prophets ───────────────────────────────────────────────────────
    (r'Hosea|Hos',                                         'Hos',      'hosea'),
    (r'Joel',                                              'Joel',     'joel'),
    (r'Amos|(?<=\s)Am(?=\.\s*\d)',                        'Am',       'amos'),
    (r'Obad(?:ja)?|Abd',                                   'Ob',       'obadiah'),
    (r'Jonas?',                                            'Jona',     'jonah'),
    (r'Micha?|Mic',                                        'Mi',       'micah'),
    (r'Nahum|Nah',                                         'Nah',      'nahum'),
    (r'Habak(?:kuk|uc)|Hab',                              'Hab',      'habakkuk'),
    (r'Zephan(?:ias?)?|Soph',                              'Zef',      'zephaniah'),
    (r'Haggai|Hag',                                        'Hag',      'haggai'),
    (r'S(?:a|e)ch(?:aria)?|Zach',                         'Sach',     'zechariah'),
    (r'Malachi?|Mal',                                      'Mal',      'malachi'),
    # ── NT ───────────────────────────────────────────────────────────────────
    (r'Matt?h?(?:\.?(?:ä|ae)us|ei|ai)?',                 'Mt',       'matthew'),
    (r'Marc(?:us|i)?|Mark(?:us)?',                        'Mk',       'mark'),
    (r'Luc(?:ae|as)?|Lukas',                              'Lk',       'luke'),
    (r'Apg|Act(?:a|us)?|Apostel\s*Geschicht(?:e)?|Apost', 'Apg',     'acts'),
    (r'R(?:ö|oe)m(?:er)?|Rom',                            'Roem',     'romans'),
    (r'2\.?\s*C(?:o|ö|oe)r(?:inth(?:ier|er)?)?',        '2Kor',     '2corinthians'),
    (r'1\.?\s*C(?:o|ö|oe)r(?:inth(?:ier|er)?)?',        '1Kor',     '1corinthians'),
    (r'Galat(?:er|ien)?|Gal',                             'Gal',      'galatians'),
    (r'Ephes(?:er|ier)?|Eph',                             'Eph',      'ephesians'),
    (r'Philem(?:on)?|Phlm',                               'Phlm',     'philemon'),
    (r'Philipp(?:er)?|Phil',                              'Phil',     'philippians'),
    (r'Coloss(?:er|ier)?|Col|Kol',                       'Kol',      'colossians'),
    (r'2\.?\s*Thess(?:alonich(?:er)?)?',                 '2Thess',   '2thessalonians'),
    (r'1\.?\s*Thess(?:alonich(?:er)?)?',                 '1Thess',   '1thessalonians'),
    (r'2\.?\s*Tim(?:oth(?:eus|ei|ii)?)?',                '2Tim',     '2timothy'),
    (r'1\.?\s*Tim(?:oth(?:eus|ei|ii)?)?',                '1Tim',     '1timothy'),
    (r'Titus|Tit',                                         'Tit',      'titus'),
    (r'H(?:e|ä)br(?:\.?(?:äer|aeer|aer))?|Heb',         'Hebr',     'hebrews'),
    (r'Jac(?:ob(?:us|i)?)?|Jak(?:ob(?:us|i)?)?',        'Jak',      'james'),
    (r'2\.?\s*Pet(?:r(?:us|i)?)?',                       '2Petr',    '2peter'),
    (r'1\.?\s*Pet(?:r(?:us|i)?)?',                       '1Petr',    '1peter'),
    (r'Judas?(?!\s*Isch)|Jud(?!ic|as?\s*Isch)',          'Jud',      'jude'),
    (r'3\.?\s*Joh(?:ann(?:is|es|em|i)?)?|3\.?\s*Johan', '3Joh',     '3john'),
    (r'2\.?\s*Joh(?:ann(?:is|es|em|i)?)?|2\.?\s*Johan', '2Joh',     '2john'),
    (r'1\.?\s*Joh(?:ann(?:is|es|em|i)?)?|1\.?\s*Johan', '1Joh',     '1john'),
    (r'Joh(?:ann(?:is|es|em|i)?)?|Johan',                'Joh',      'john'),
    (r'Offenbarung|Apoc(?:al(?:ypsis|ypse)?)?|Offb',    'Offb',     'revelation'),
]

# Genesis=50 Mose numbers → API slug
_MOSE_SLUGS = {
    '1': 'genesis', '2': 'exodus', '3': 'leviticus',
    '4': 'numbers', '5': 'deuteronomy',
}

# ─────────────────────────────────────────────────────────────────────────────
# 2.  Assemble master extraction regex
# ─────────────────────────────────────────────────────────────────────────────
_NUM    = r'\d{1,3}'
_CHNUM  = r'(?:\d{1,3}|[IVXLC]{1,6}(?=[.,;\s]))'
_SEP    = r'[\s:.,;]*'
_CHAP   = r'(?:am|im|an|in|Cap\.?|cap\.?|Capit(?:tel|el|l[se]?)\.?|Kap\.?|c\.(?=\s*\d))?'
_VERSE  = r'(?:[\s.,;]*(?:v\.|vers?\.?|V\.)[\s.]*\d{1,3})?'
_RANGE  = r'(?:[-\u2013\u2014]\d{1,3})?'

_book_alt = '|'.join(r'(?:' + pat + r')' for pat, _, __ in BOOKS)

_PAT_A = (
    r'(?<!\w)(?:' + _book_alt + r')\.?'
    + _SEP + _CHAP + _SEP + _CHNUM + _RANGE + _VERSE
)
_PSALM_VAR = r'(?:Psal(?:m(?:en|o|us)?|ter)?|Pfalm|Psal)'
_PAT_B     = (r'(?:(?:im|in|am|den|dem|des|der|die|das)\s+)?'
              + _NUM + r'\.\s*' + _PSALM_VAR)
_CAP_VAR   = r'(?:Capitl(?:s|e)?|Capitel|Capittel|Cap)'
_PAT_C     = (_NUM + r'\.?\s*' + _CAP_VAR + r'\s*(?:des\s+Buchs?\s+)?(?:' + _book_alt + r')')

MASTER_RE = re.compile(
    r'(?:' + _PAT_A + r'|' + _PAT_B + r'|' + _PAT_C + r')',
    re.IGNORECASE | re.UNICODE
)

_MAX_CHAPTER = 150   # Psalms is the longest (150); anything higher = noise

# ─────────────────────────────────────────────────────────────────────────────
# 3.  Roman-numeral converter
# ─────────────────────────────────────────────────────────────────────────────
_ROM_VALS = {'I':1,'V':5,'X':10,'L':50,'C':100,'D':500,'M':1000}

def roman_to_int(s: str) -> int:
    result, prev = 0, 0
    for ch in reversed(s.upper()):
        v = _ROM_VALS.get(ch, 0)
        result += v if v >= prev else -v
        prev = v
    return result

# ─────────────────────────────────────────────────────────────────────────────
# 4.  Citation parser  → (book_en, chapter_int, verse_int_or_None)
# ─────────────────────────────────────────────────────────────────────────────

# Pre-compile per-book patterns for parse_citation()
_BOOK_RES = [
    (re.compile(r'^(?:' + pat + r')\.?', re.I | re.U), label, slug)
    for pat, label, slug in BOOKS
]

_RE_VERSE_MARK = re.compile(r'(?:v\.|vers?\.?|V\.)\s*(\d+)', re.I)
_RE_CHAP_MARK  = re.compile(r'(?:am|im|an|in|Cap\.?|cap\.?|Capit\w*\.?|Kap\.?|c\.)\s*', re.I)
_RE_ARABIC     = re.compile(r'\d+')
_RE_ROMAN      = re.compile(r'[IVXLC]{1,6}(?=[.,;\s]|$)', re.I)
_RE_FIRST_NUM  = re.compile(r'\d+')

def _first_number(text: str):
    """Return first Arabic number in text as int, or None."""
    m = _RE_ARABIC.search(text)
    return int(m.group()) if m else None

def _extract_chap_verse(after_book: str):
    """Given text after the book name, return (chapter_int, verse_int_or_None)."""
    # Strip leading separators and optional chapter connector
    s = _SEP_RE.sub('', after_book.lstrip())
    s = _RE_CHAP_MARK.sub('', s).strip(' .,;:')

    verse = None
    vm = _RE_VERSE_MARK.search(after_book)
    if vm:
        verse = int(vm.group(1))

    # Try Arabic chapter
    am = _RE_ARABIC.match(s)
    if am:
        return int(am.group()), verse
    # Try Roman numeral
    rm = _RE_ROMAN.match(s)
    if rm:
        return roman_to_int(rm.group()), verse
    return None, verse

_SEP_RE = re.compile(r'^[\s:.,;]+')

def _mose_slug(citation: str) -> str:
    """Detect which Mose book from a leading digit in the citation."""
    m = re.match(r'^(\d)', citation)
    if m:
        return _MOSE_SLUGS.get(m.group(1), 'genesis')
    # Try ordinal German words
    ord_map = {
        'erst': 'genesis', 'ander': 'exodus', 'zweit': 'exodus',
        'drit': 'leviticus', 'viert': 'numbers', 'fünft': 'deuteronomy',
        'fuenft': 'deuteronomy',
    }
    low = citation.lower()
    for k, v in ord_map.items():
        if k in low:
            return v
    return 'genesis'   # fallback

def parse_citation(citation: str):
    """
    Return (book_en_slug, chapter_int, verse_int_or_None).
    book_en_slug is None when the book cannot be identified.
    """
    cit = citation.strip()

    # ── Pattern B (reverse Psalm): "91. Psalmen" / "im 91. Psalm" ────────────
    mb = re.match(
        r'^(?:(?:im|in|am|den|dem|des|der|die|das)\s+)?(\d+)\.\s*' + _PSALM_VAR,
        cit, re.I
    )
    if mb:
        return 'psalms', int(mb.group(1)), None

    # ── Pattern C (chapter+book): "37. Capitel des Buchs Hiobo" ─────────────
    mc = re.match(r'^(\d+)\.?\s*' + _CAP_VAR + r'\s*(?:des\s+Buchs?\s+)?(.*)', cit, re.I)
    if mc:
        chap = int(mc.group(1))
        rest = mc.group(2).strip()
        # identify book from rest
        for bk_re, label, slug in _BOOK_RES:
            if bk_re.match(rest):
                if slug is None and label in ('MoseN', 'Mose'):
                    slug = _mose_slug(rest)
                return slug, chap, None
        return None, chap, None

    # ── Pattern A (main): BOOK ... CHAPTER [VERSE] ───────────────────────────
    for bk_re, label, slug in _BOOK_RES:
        m = bk_re.match(cit)
        if m:
            after = cit[m.end():]
            if slug is None:
                if label in ('MoseN', 'Mose'):
                    slug = _mose_slug(cit)
                # else stays None (unknown)
            chap, verse = _extract_chap_verse(after)
            return slug, chap, verse

    return None, None, None

# ─────────────────────────────────────────────────────────────────────────────
# 5.  Extraction helpers
# ─────────────────────────────────────────────────────────────────────────────
_WS = re.compile(r'\s+')

def clean(s: str) -> str:
    return _WS.sub(' ', s).strip(' .,;:/')

def extract_citations(text: str) -> list:
    if not text:
        return []
    seen, result = set(), []
    for m in MASTER_RE.finditer(text):
        raw = clean(m.group())
        if not any(c.isdigit() for c in raw):
            continue
        if len(raw) < 4:
            continue
        fn = _RE_FIRST_NUM.search(raw)
        if fn and int(fn.group()) > _MAX_CHAPTER:
            continue
        if raw not in seen:
            seen.add(raw)
            result.append(raw)
    return result

# ─────────────────────────────────────────────────────────────────────────────
# 6.  Main
# ─────────────────────────────────────────────────────────────────────────────
rows = []
json_files = sorted(p for p in Path(JSON_DIR).iterdir() if p.suffix == '.json')

for path in json_files:
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except Exception as exc:
        print(f'[warn] {path.name}: {exc}')
        continue

    slug = data.get('slug', path.stem)
    text = data.get('text', '')
    cits = extract_citations(text)
    print(f'{path.name}: {len(cits)} citation(s)')

    for c in cits:
        book_en, chapter, verse = parse_citation(c)
        if chapter and chapter > _MAX_CHAPTER:
            chapter = None          # safety net
        url = ''
        if book_en and chapter:
            v = verse if verse else 1
            url = API_BASE.format(book=book_en, ch=chapter, v=v)
        rows.append({
            'SLUG':          slug,
            'BIBLE CITATION': c,
            'BOOK_EN':       book_en or '',
            'CHAPTER':       chapter or '',
            'VERSE':         verse   or '',
            'URL':           url,
        })

FIELDS = ['SLUG', 'BIBLE CITATION', 'BOOK_EN', 'CHAPTER', 'VERSE', 'URL']
with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as fh:
    writer = csv.DictWriter(fh, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)

print(f'\nDone — {len(rows)} citations -> {OUTPUT_CSV}')

# Quick quality summary
no_book = sum(1 for r in rows if not r['BOOK_EN'])
no_chap = sum(1 for r in rows if not r['CHAPTER'])
has_ver = sum(1 for r in rows if r['VERSE'])
print(f'  with book:    {len(rows)-no_book}/{len(rows)}')
print(f'  with chapter: {len(rows)-no_chap}/{len(rows)}')
print(f'  with verse:   {has_ver}/{len(rows)}')
