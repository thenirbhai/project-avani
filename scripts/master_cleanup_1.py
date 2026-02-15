"""
master_cleanup_colab.py
=======================
Step 2: Process the exported raw text file with the full cleanup pipeline.

Reads sangraha_raw.txt (one document per line, newlines escaped as \\n),
cleans each document in parallel, and writes the result.

Pipeline:
  1. ZWJ / Chillu normalization
  2. Structural cleanup (char filtering, stray vowels, punctuation, whitespace)
  3. Boilerplate removal (keywords, regex, heuristics)

Optimizations:
  • str.translate()   — C-level character filtering (~10× vs generator)
  • multiprocessing   — parallel cleanup across all CPU cores
  • Batched I/O       — read/write in chunks to reduce syscalls
  • No HF dependency  — pure file I/O, no Arrow deserialization overhead
"""

import os
import re
import time
import unicodedata
from multiprocessing import Pool, cpu_count
from tqdm.auto import tqdm

# ════════════════════════════════════════════════════════════════════
# 1. ZWJ / CHILLU NORMALIZATION
# ════════════════════════════════════════════════════════════════════

VIRAMA = '\u0D4D'
ZWJ    = '\u200D'

_CHILLU_PAIRS = [
    ('\u0D23' + VIRAMA + ZWJ, '\u0D7A'),  # ണ → Chillu NN
    ('\u0D28' + VIRAMA + ZWJ, '\u0D7B'),  # ന → Chillu N
    ('\u0D30' + VIRAMA + ZWJ, '\u0D7C'),  # ര → Chillu RR
    ('\u0D32' + VIRAMA + ZWJ, '\u0D7D'),  # ല → Chillu L
    ('\u0D33' + VIRAMA + ZWJ, '\u0D7E'),  # ള → Chillu LL
]


def normalize_zwj(text: str) -> str:
    """Replace legacy Consonant+Virama+ZWJ with atomic Chillu codepoints."""
    for old, new in _CHILLU_PAIRS:
        if old in text:
            text = text.replace(old, new)
    return text


# ════════════════════════════════════════════════════════════════════
# 2. STRUCTURAL CLEANUP
# ════════════════════════════════════════════════════════════════════

ALLOWED_EXTRA = frozenset([
    '\u00B0',  # ° degree sign
    '\u00A9',  # © copyright
    '\u00AE',  # ® registered
    '\u00BD',  # ½
    '\u00BC',  # ¼
    '\u20B9',  # ₹ Indian Rupee
    '\u2026',  # … ellipsis
    '\u200D',  # ZWJ  (preserved)
    '\u200C',  # ZWNJ (preserved)
])

_ALLOWED_CP: frozenset = frozenset(
    list(range(0x0D00, 0x0D80))          # Malayalam block
    + list(range(0x0020, 0x007F))        # Printable ASCII
    + [ord('\t'), ord('\n'), ord('\r')]   # Whitespace controls
    + [ord(c) for c in ALLOWED_EXTRA]
)


def _filter_chars(text: str) -> str:
    """Remove disallowed characters using C-level str.translate()."""
    to_delete = {ord(c) for c in set(text)} - _ALLOWED_CP
    if not to_delete:
        return text
    return text.translate({cp: None for cp in to_delete})


# --- Stray vowel-sign removal ---------------------------------------
_VOWEL_SIGNS = frozenset(chr(cp) for cp in range(0x0D3E, 0x0D4E))
_VOWEL_SIGNS_WITH_AU = _VOWEL_SIGNS | {'\u0D57'}

_VALID_BASES = (
    frozenset(chr(cp) for cp in range(0x0D15, 0x0D3A))   # consonants
    | frozenset(chr(cp) for cp in range(0x0D7A, 0x0D80))  # chillus
    | _VOWEL_SIGNS_WITH_AU
)


def _remove_stray_vowel_signs(text: str) -> str:
    """Drop Malayalam vowel signs that lack a valid base."""
    out: list[str] = []
    prev = None
    for ch in text:
        if ch in _VOWEL_SIGNS_WITH_AU and (prev is None or prev not in _VALID_BASES):
            continue
        out.append(ch)
        prev = ch
    return ''.join(out)


# --- Regex patterns (compiled once at import time) -------------------
_RE_ELLIPSIS        = re.compile(r'[.…]{2,}')
_RE_REPEATED_PUNCT  = re.compile(r'([.!?,;:\-*#])\1+')
_RE_SPACED_PUNCT    = re.compile(r'([.!?,;:])([\s]*[.!?,;:])+')   # `. .` or `.! ! !`
_RE_HTML_TAGS       = re.compile(r'</?[a-zA-Z][^>]*/?>')           # <ref>, </ref>, <br/>, etc.
_RE_MULTI_SPACE     = re.compile(r'[ \t]{2,}')
_RE_MULTI_BLANK     = re.compile(r'\n{3,}')
_RE_ASTERISK_LINE   = re.compile(r'^[\s*]+$')


def structural_cleanup(text: str) -> str:
    """Full structural cleanup on a single document."""
    text = unicodedata.normalize('NFC', text)
    text = text.replace('\ufeff', '')          # Remove BOM / ZWNBS
    text = _RE_HTML_TAGS.sub('', text)         # Strip HTML/XML tags
    text = _filter_chars(text)
    text = _remove_stray_vowel_signs(text)
    text = _RE_ELLIPSIS.sub('.', text)
    text = _RE_REPEATED_PUNCT.sub(r'\1', text)
    text = _RE_SPACED_PUNCT.sub(r'\1', text)   # `. .` → `.`, `.! ! !` → `.`
    text = _RE_MULTI_SPACE.sub(' ', text)

    lines = text.split('\n')
    lines = [ln.strip() for ln in lines]
    lines = [ln for ln in lines if ln and not _RE_ASTERISK_LINE.match(ln)]
    text = '\n'.join(lines)
    text = _RE_MULTI_BLANK.sub('\n\n', text)
    return text


# ════════════════════════════════════════════════════════════════════
# 3. BOILERPLATE REMOVAL
# ════════════════════════════════════════════════════════════════════

BOILERPLATE_KEYWORDS = [kw.lower() for kw in [
    "©", "copyright", "all rights reserved", "terms of use",
    "privacy policy", "disclaimer", "cookie policy",
    "also read", "don't miss", "don\u2019t miss", "read more", "click here",
    "read also",
    "subscribe", "sign up", "log in", "share this",
    "follow us", "join us", "see also", "previous:", "next:",
    "load more", "show more", "view more",
    "last modified", "last updated", "english edition",
    "published:", "updated:",
    "മറുനാടൻ ടിവിയുടെ ഫേസ്ബുക്ക് പേജ് ഹാക്ക് ചെയ്തു",
    "ഷാജൻ സ്കറിയയുടെ വീഡിയോ കാണാം",
    "കൂടുതൽ വായിക്കുക", "തുടർന്ന് വായിക്കുക",
    "സബ്സ്ക്രൈബ് ചെയ്യുക", "ഷെയർ ചെയ്യുക", "കമന്റ് ചെയ്യുക",
    "ലൈക്ക് ചെയ്യുക", "സെർച്ച്",
    "advertisement", "sponsored", "ad ",
    "keywords:", "top-headlines",
    "begin typing your search above",
    "your comment added successfully",
    "consectetur adipiscing elit",
    "save my name, email, and website",
    "press ctrl+m to toggle",
    "download the fanport app",
    # Language selector boilerplate
    "ഭാഷ തിരഞ്ഞെടുക്കുക",
    "കൂടുതൽ ഭാഷ",
]]

_BOILERPLATE_RE = [re.compile(p, re.IGNORECASE) for p in [
    r"©.*\d{4}",
    r"Copyright.*\d{4}",
    r"All\s+rights\s+reserved",
    r"^-\s*(News|Technology|Sports|Entertainment|Business|World|National|India)\s",
    r"^\s*[\-\|•·=]{3,}\s*$",
    r"^\s*\|?\s*\d{1,2}\s*[A-Za-z]+\s*\d{4}\s*$",
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*,?\s+\d",
    r"https?://\S+",
    r"Archived\s+\d{4}-\d{2}-\d{2}\s+at\s+the\s+Wayback\s+Machine",
    r"^[^|]*\|[^|]*\|[^|]*\|",
    r"^-\s+.+;\s+.+;\s+.+;\s+.+;\s+",
    r'\+\d{1,4}[\s.-]?\d{1,4}[\s.-]?\d{1,4}[\s.-]?\d{1,9}',

    # Parenthesized language names:  (Hindi), (Malayalam), (Marathi), etc.
    r'^\s*\(?\s*(Hindi|Marathi|Gujarati|Kannada|Bengali|Malayalam|Telugu|Punjabi|Urdu|Odia|Assamese|Tamil|English|Sanskrit)\s*\)?\)?\s*$',

    # Lines that are ONLY punctuation, symbols, spaces (no letters at all)
    # e.g. ' (). () ' '' , !   or   (). ()
    r"^[\s\(\)\[\]\{\}'.,:;!?\-_/*#@&|=+<>~`\\\"]+$",
]]

_RE_ML_RANGE  = re.compile(r'[\u0D00-\u0D7F]')
_RE_ALPHA     = re.compile(r'[A-Za-z\u0D00-\u0D7F]')
_RE_LETTERS   = re.compile(r'[A-Za-z\u0D00-\u0D7F]')  # any letter


def _malayalam_ratio(line: str) -> float:
    alpha = _RE_ALPHA.findall(line)
    if not alpha:
        return 0.0
    return len(_RE_ML_RANGE.findall(line)) / len(alpha)


def _letter_ratio(line: str) -> float:
    """Fraction of characters that are actual letters (not symbols/punct)."""
    if not line:
        return 0.0
    letters = len(_RE_LETTERS.findall(line))
    return letters / len(line)


def _is_boilerplate_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True

    lower = stripped.lower()
    for kw in BOILERPLATE_KEYWORDS:
        if kw in lower:
            return True

    for pat in _BOILERPLATE_RE:
        if pat.search(stripped):
            return True

    # Very short lines (< 5 chars) are noise
    if 0 < len(stripped) < 5:
        return True

    # Lines with very low letter content (mostly symbols/punctuation)
    # e.g. " ' (). () ' '' , ! " → almost no real letters
    if len(stripped) > 3 and _letter_ratio(stripped) < 0.3:
        return True

    # Short-to-medium lines with ZERO Malayalam characters → noise
    # (allows long English lines that may be quotes or references)
    ml_chars = len(_RE_ML_RANGE.findall(stripped))
    if ml_chars == 0 and len(stripped) < 80:
        return True

    # Longer lines that are mostly English (< 15% Malayalam)
    if len(stripped) > 30 and _malayalam_ratio(stripped) < 0.15:
        return True

    return False


def remove_boilerplate(text: str) -> str:
    lines = text.split('\n')
    kept = [ln for ln in lines if not _is_boilerplate_line(ln)]
    return '\n'.join(kept)


# ════════════════════════════════════════════════════════════════════
# 4. UNIFIED PIPELINE
# ════════════════════════════════════════════════════════════════════

MIN_CLEANED_LENGTH = 20


def clean_document(raw_line: str) -> str | None:
    """Clean a single raw line (escaped doc) from the text file.

    Expects the line format from export_dataset.py:
      - one document per line
      - internal newlines escaped as literal \\n
    Returns cleaned text, or None if too short / empty.
    """
    # Unescape: literal \\n back to real newlines
    text = raw_line.strip().replace('\\n', '\n')
    if not text:
        return None

    text = normalize_zwj(text)
    text = structural_cleanup(text)
    text = remove_boilerplate(text)
    text = text.strip()

    if len(text) < MIN_CLEANED_LENGTH:
        return None
    return text


# ════════════════════════════════════════════════════════════════════
# 5. MAIN — READ FILE, PARALLEL CLEAN, WRITE
# ════════════════════════════════════════════════════════════════════

INPUT_FILE    = '../data/wikipedia_mal_raw.txt'
OUTPUT_FILE   = '../data/wikipedia_mal_cleaned_v1.txt'
DOC_SEPARATOR = '\n\n'            # double newline between documents
REPORT_EVERY  = 50_000
WRITE_BUFFER  = 5_000
NUM_WORKERS   = max(1, cpu_count() - 1)
CHUNK_SIZE    = 512


def _count_lines(path: str) -> int:
    """Fast line count using buffered raw reads."""
    count = 0
    with open(path, 'rb') as f:
        buf_size = 1 << 20  # 1 MB buffer
        while True:
            buf = f.read(buf_size)
            if not buf:
                break
            count += buf.count(b'\n')
    return count


def _line_reader(path: str):
    """Generator that yields stripped lines from the file."""
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            yield line


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: Input file not found: {INPUT_FILE}")
        print(f"       Run export_dataset.py first to create it.")
        return

    t0 = time.time()

    # Fast line count for progress bar
    print(f"Counting documents in {INPUT_FILE}...")
    total_docs = _count_lines(INPUT_FILE)
    print(f"  Total documents: {total_docs:,}")
    print(f"  Workers:         {NUM_WORKERS}")
    print()

    kept_docs    = 0
    dropped_docs = 0
    write_buf: list[str] = []

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out, \
         Pool(NUM_WORKERS) as pool:

        results = pool.imap(clean_document, _line_reader(INPUT_FILE),
                            chunksize=CHUNK_SIZE)

        for i, cleaned in enumerate(
            tqdm(results, total=total_docs, desc="Cleaning", unit=" docs"), 1
        ):
            if cleaned is not None:
                # Keep real newlines — LLM-training-friendly
                write_buf.append(cleaned)
                kept_docs += 1
            else:
                dropped_docs += 1

            if len(write_buf) >= WRITE_BUFFER:
                f_out.write(DOC_SEPARATOR.join(write_buf) + DOC_SEPARATOR)
                write_buf.clear()

            if i % REPORT_EVERY == 0:
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                print(
                    f"  [{i:>10,} / {total_docs:,}]  "
                    f"kept={kept_docs:,}  dropped={dropped_docs:,}  "
                    f"({rate:,.0f} docs/s)"
                )

        # Flush remaining
        if write_buf:
            f_out.write(DOC_SEPARATOR.join(write_buf) + DOC_SEPARATOR)

    elapsed = time.time() - t0

    # ── Final report ──────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"{'CLEANUP COMPLETE':^60}")
    print("=" * 60)
    print(f"  Total documents processed:  {total_docs:>12,}")
    print(f"  Documents kept:             {kept_docs:>12,}")
    print(f"  Documents dropped:          {dropped_docs:>12,}")
    if total_docs:
        print(f"  Keep rate:                  {kept_docs / total_docs * 100:>11.1f}%")
    print(f"  Wall time:                  {elapsed:>11.1f}s")
    if elapsed > 0:
        print(f"  Throughput:                 {total_docs / elapsed:>11,.0f} docs/s")
    print(f"\n  Output file: {OUTPUT_FILE}")
    print("=" * 60)


if __name__ == '__main__':
    main()
