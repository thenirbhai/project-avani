"""
wikipedia_cleaning.py
=====================
Second-pass cleaner for Wikipedia Malayalam data.

Reads  : data/wikipedia_mal_cleaned_v1.txt 
Writes : data/wikipedia_mal_cleaned_v2.txt

Cleaning phases:
  Phase 1 — High-impact line removal  (~33 % of file)
      1.1  Wikipedia category tags  (വർഗ്ഗം:)
      1.2  Table / infobox fragments (pipe-separated, wiki table markers)
      1.3  Short fragment lines      (< MIN_LINE_LENGTH chars)
      1.4  Empty / whitespace-only lines  → collapsed

  Phase 2 — Wiki markup cleanup
      2.1  Wiki link syntax  [[Target|Display]] → Display
      2.2  Template remnants {{…}}
      2.3  Section header markers  == … ==
      2.4  Stub / incomplete-article markers

  Phase 3 — Content quality filters
      3.1  Coordinate-only lines
      3.2  Date-only lines
      3.3  Bullet / numbered-list markers  (strip marker, keep content)
      3.4  LaTeX / math markup ($\\text, \\displaystyle, etc.)
      3.5  Empty bracket / paren lines ([] [], ( ), etc.)
      3.6  Wikipedia image parameters (upright=, thumb|, 250px|)
      3.7  File paths (%%windir%%, C:\\, /usr/, etc.)
      3.8  Phonetic / transliteration notation (/t~t/, /ai/, etc.)
      3.9  Ref tags (<ref ...>, </ref>) -- stripped inline
      3.10 URLs (http://...) -- stripped inline

  Phase 4 — Document-level quality
      4.1  Minimum document length
      4.2  Paragraph coherence (at least one line > 30 chars)

Optimised for large files:
  • Streaming line-by-line reader — constant memory
  • multiprocessing.Pool with batched I/O
  • Compiled regex patterns (module-level)
"""

import os
import re
import time
from multiprocessing import Pool, cpu_count

# ════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════

INPUT_FILE  = os.path.join(os.path.dirname(__file__), '..', 'data', 'wikipedia_mal_cleaned_v1.txt')
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'wikipedia_mal_cleaned_v2.txt')

MIN_LINE_LENGTH   = 10      # lines shorter than this (chars) are removed
MIN_DOC_LENGTH    = 50      # documents shorter than this (chars) are dropped
MIN_PROSE_LINE    = 30      # at least one line must be this long for coherence
DOC_SEPARATOR     = '\n\n'  # double newline between documents in the file
WRITE_BUFFER      = 5_000   # flush output every N documents
NUM_WORKERS       = max(1, cpu_count() - 1)
CHUNK_SIZE        = 512
REPORT_EVERY      = 50_000

# ════════════════════════════════════════════════════════════════════
# COMPILED REGEX PATTERNS
# ════════════════════════════════════════════════════════════════════

# Phase 1
_RE_CATEGORY       = re.compile(r'^\[?\[?\s*വർഗ്ഗം:', re.UNICODE)
_RE_TABLE_MARKER   = re.compile(r'^\s*[\{\|\}!\-\+]')
_RE_PIPE_ROW       = re.compile(r'\|.*\|.*\|')
_RE_PIPE_START     = re.compile(r'^\s*\|')

# Phase 2
_RE_WIKI_LINK      = re.compile(r'\[\[(?:[^\]]*\|)?([^\]]+)\]\]')
_RE_TEMPLATE       = re.compile(r'\{\{[^}]*\}\}')
_RE_SECTION_HDR    = re.compile(r'^(=+)\s*(.+?)\s*=+\s*$')
_RE_STUB           = re.compile(
    r'(അപൂർണ്ണമാണ്|അപൂർണമാണ്|അപൂർണ്ണം|അപൂർണം|stub)',
    re.IGNORECASE | re.UNICODE
)

# Phase 3
_RE_COORD_ONLY     = re.compile(r'^[\d\s°\'\"NSEW,.\-]+$')
_RE_BARE_YEAR      = re.compile(r'^\d{4}\s*$')
_RE_DATE_ONLY      = re.compile(r'^\d{1,2}\s+\S+\s*\d{0,4}\s*$')
_RE_BULLET         = re.compile(r'^[\*#]+\s*')

# Phase 3 — Additional noise patterns (user-identified)
_RE_LATEX          = re.compile(r'\$\\?\w|\\displaystyle|\\text\{|\\frac\{|\\mathbf|\\mathrm|\\begin\{|\\end\{')
_RE_EMPTY_BRACKETS = re.compile(r'^[\s\[\](){}]*$')               # lines that are ONLY brackets/parens
_RE_WIKI_IMAGE     = re.compile(r'(upright\s*=|\|\s*thumb\s*\||\d+px\s*\||alt\s*=|frame\s*\||frameless\s*\|)')
_RE_FILE_PATH      = re.compile(r'(%\w+%|[A-Z]:\\|/usr/|/etc/|/var/|\\debug\\|\\system32)')
_RE_PHONETIC       = re.compile(                                   # phonetic / IPA notation
    r'^[\s]*(/[a-zA-Z~.ːˈˌ]+/[\s,]*)+$'      # /phoneme/ lines (1 or more tokens)
    r'|^[\s]*(\[[a-zA-Z~.ːˈˌ]+\][\s,]*)+$'   # [phoneme] lines (e.g. [p], [t])
)
_RE_REF_TAG        = re.compile(r'</?ref[^>]*/?>')                 # <ref ...> or </ref>
_RE_URL            = re.compile(r'https?://\S+')

# Phase 3 — Specific inline noise removal (merged from clean_raw_data.py)
_RE_INLINE_BRACKETS = re.compile(r'\[[pc]?\]')                    # [], [p], [c]
_RE_INLINE_PHONETIC = re.compile(r'/(?:[a-zA-Z~]+)/|/i~i~ai~aj~ej~oj/|/~~/') # /ai/, /au/, etc.

# General
_RE_MULTI_BLANK    = re.compile(r'\n{3,}')
_RE_MULTI_SPACE    = re.compile(r'[ \t]{2,}')


# ════════════════════════════════════════════════════════════════════
# LINE-LEVEL CLEANING
# ════════════════════════════════════════════════════════════════════

def _should_remove_line(line: str) -> bool:
    """Return True if the line should be entirely discarded."""
    stripped = line.strip()

    # Empty / whitespace-only
    if not stripped:
        return True

    # Phase 1.1 — Category tags
    if _RE_CATEGORY.match(stripped):
        return True

    # Phase 1.2 — Table / infobox fragments
    if _RE_TABLE_MARKER.match(stripped):
        return True
    if _RE_PIPE_ROW.search(stripped):
        return True
    if _RE_PIPE_START.match(stripped):
        return True

    # Phase 1.3 — Short fragments
    if len(stripped) < MIN_LINE_LENGTH:
        return True

    # Phase 2.4 — Stub markers
    if _RE_STUB.search(stripped):
        return True

    # Phase 3.1 — Coordinate-only lines
    if _RE_COORD_ONLY.match(stripped) and len(stripped) > 2:
        return True

    # Phase 3.2 — Date-only lines
    if _RE_BARE_YEAR.match(stripped):
        return True
    if _RE_DATE_ONLY.match(stripped):
        return True

    # Phase 3 — Additional noise patterns
    # LaTeX / math markup
    if _RE_LATEX.search(stripped):
        return True

    # Lines that are only empty brackets / parens:  [] [] , ( ), { } etc.
    if _RE_EMPTY_BRACKETS.match(stripped):
        return True

    # Wikipedia image/file parameters:  upright=1.13|thumb|  , 250px|
    if _RE_WIKI_IMAGE.search(stripped):
        return True

    # Windows / Unix file paths:  %windir%\debug\mrt.log , C:\Users\
    if _RE_FILE_PATH.search(stripped):
        return True

    # Phonetic / transliteration notation:  /t~t/ /d~d/ /ai/ /au/
    if _RE_PHONETIC.search(stripped):
        return True

    return False


def _clean_line(line: str) -> str | None:
    """Clean a single line: remove noise, strip markup, return cleaned text or None."""
    if _should_remove_line(line):
        return None

    stripped = line.strip()

    # Phase 2.1 — Strip wiki link syntax  [[Target|Display]] → Display
    if '[[' in stripped:
        stripped = _RE_WIKI_LINK.sub(r'\1', stripped)

    # Phase 2.2 — Remove template remnants  {{…}}
    if '{{' in stripped:
        stripped = _RE_TEMPLATE.sub('', stripped)

    # Phase 2.3 — Remove section header markers  == Section == → Section
    hdr_match = _RE_SECTION_HDR.match(stripped)
    if hdr_match:
        stripped = hdr_match.group(2).strip()
        # After stripping markers, short headers get removed
        if len(stripped) < MIN_LINE_LENGTH:
            return None

    # Phase 3.3 — Strip bullet / list markers, keep content
    if _RE_BULLET.match(stripped):
        stripped = _RE_BULLET.sub('', stripped).strip()
        if len(stripped) < MIN_LINE_LENGTH:
            return None

    # Strip ref tags inline:  <ref name="...">...</ref>  → remove
    if '<ref' in stripped.lower() or '</ref' in stripped.lower():
        stripped = _RE_REF_TAG.sub('', stripped)

    # Strip URLs inline:  http://example.com  → remove
    if 'http' in stripped:
        stripped = _RE_URL.sub('', stripped)

    # Phase 3 — Specific inline noise removal
    # Remove brackets [], [p], [c]
    if '[' in stripped:
         stripped = _RE_INLINE_BRACKETS.sub('', stripped)

    # Remove phonetic markers /ai/, /au/, etc.
    if '/' in stripped:
         stripped = _RE_INLINE_PHONETIC.sub('', stripped)

    # Collapse multiple spaces
    stripped = _RE_MULTI_SPACE.sub(' ', stripped).strip()

    # Final length check after all transformations
    if not stripped or len(stripped) < MIN_LINE_LENGTH:
        return None

    return stripped


# ════════════════════════════════════════════════════════════════════
# DOCUMENT-LEVEL CLEANING
# ════════════════════════════════════════════════════════════════════

def clean_document(raw_doc: str) -> str | None:
    """
    Clean a single document (block of text separated by double newlines).

    Returns cleaned text or None if the document doesn't pass quality checks.
    """
    lines = raw_doc.split('\n')
    cleaned_lines = []

    for line in lines:
        result = _clean_line(line)
        if result is not None:
            cleaned_lines.append(result)

    if not cleaned_lines:
        return None

    text = '\n'.join(cleaned_lines)

    # Phase 4.1 — Minimum document length
    if len(text.strip()) < MIN_DOC_LENGTH:
        return None

    # Phase 4.2 — Paragraph coherence: at least one line should be prose-length
    has_prose = any(len(ln) >= MIN_PROSE_LINE for ln in cleaned_lines)
    if not has_prose:
        return None

    return text.strip()


# ════════════════════════════════════════════════════════════════════
# FILE I/O — DOCUMENT READER
# ════════════════════════════════════════════════════════════════════

def _document_reader(path: str):
    """
    Generator that yields documents from the file.
    Documents are separated by double newlines (blank lines).
    """
    current_doc_lines: list[str] = []

    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')

            # A blank line may signal document boundary
            if not line.strip():
                if current_doc_lines:
                    # Check if this is a real document boundary (2+ blank lines)
                    # or just a paragraph break within a document
                    current_doc_lines.append('')  # preserve paragraph breaks
                continue

            # If we had accumulated blank lines and now see content,
            # check if this is a new document
            if current_doc_lines and current_doc_lines[-1] == '' and len(current_doc_lines) > 1:
                # Count trailing blanks
                trailing_blanks = 0
                for cl in reversed(current_doc_lines):
                    if cl == '':
                        trailing_blanks += 1
                    else:
                        break

                if trailing_blanks >= 2:
                    # Document boundary: yield current document (without trailing blanks)
                    doc_text = '\n'.join(
                        cl for cl in current_doc_lines if cl or trailing_blanks < 2
                    ).strip()
                    if doc_text:
                        yield doc_text
                    current_doc_lines = []

            current_doc_lines.append(line)

    # Yield the last document
    if current_doc_lines:
        doc_text = '\n'.join(current_doc_lines).strip()
        if doc_text:
            yield doc_text


def _count_documents(path: str) -> int:
    """Estimate document count by counting double-newline separators."""
    count = 0
    prev_blank = False
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                if prev_blank:
                    count += 1
                prev_blank = True
            else:
                prev_blank = False
    return count + 1  # last document has no trailing separator


def _count_lines(path: str) -> int:
    """Fast line count using buffered raw reads."""
    count = 0
    with open(path, 'rb') as f:
        while True:
            buf = f.read(1 << 20)
            if not buf:
                break
            count += buf.count(b'\n')
    return count


# ════════════════════════════════════════════════════════════════════
# SIMPLE LINE-BY-LINE MODE  (alternative: no document boundary detection)
# ════════════════════════════════════════════════════════════════════

def _line_reader(path: str):
    """Yield individual lines from the file."""
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            yield line.rstrip('\n')


def clean_lines_batch(lines: list[str]) -> list[str]:
    """Clean a batch of lines. Used for multiprocessing."""
    results = []
    for line in lines:
        cleaned = _clean_line(line)
        if cleaned is not None:
            results.append(cleaned)
    return results


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════

def main():
    input_path = os.path.abspath(INPUT_FILE)
    output_path = os.path.abspath(OUTPUT_FILE)

    if not os.path.exists(input_path):
        print(f"ERROR: Input file not found: {input_path}")
        return

    t0 = time.time()

    # Count lines for progress tracking
    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print(f"Counting lines...")
    total_lines = _count_lines(input_path)
    print(f"  Total lines: {total_lines:,}")
    print(f"  Workers:     {NUM_WORKERS}")
    print()

    # ── Stats ──
    stats = {
        'total_lines': 0,
        'kept_lines': 0,
        'removed_lines': 0,
        'removed_category': 0,
        'removed_table': 0,
        'removed_short': 0,
        'removed_empty': 0,
        'removed_latex': 0,
        'removed_brackets': 0,
        'removed_wiki_image': 0,
        'removed_file_path': 0,
        'removed_phonetic': 0,
        'removed_other': 0,
        'cleaned_wiki_links': 0,
        'cleaned_templates': 0,
        'cleaned_bullets': 0,
        'cleaned_refs': 0,
        'cleaned_urls': 0,
        'cleaned_brackets': 0,
        'cleaned_phonetic_inline': 0,
    }

    # ── Process line by line ──
    print("Cleaning...")
    write_buf: list[str] = []
    prev_was_blank = False

    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(output_path, 'w', encoding='utf-8') as f_out:

        for i, raw_line in enumerate(f_in, 1):
            line = raw_line.rstrip('\n')
            stats['total_lines'] += 1
            stripped = line.strip()

            # ── Categorise removal reason (for stats) ──
            if not stripped:
                stats['removed_empty'] += 1
                # Preserve single blank lines as paragraph breaks
                if not prev_was_blank:
                    write_buf.append('')
                    prev_was_blank = True
                stats['removed_lines'] += 1
                continue

            prev_was_blank = False

            if _RE_CATEGORY.match(stripped):
                stats['removed_category'] += 1
                stats['removed_lines'] += 1
                continue

            if (_RE_TABLE_MARKER.match(stripped) or
                _RE_PIPE_ROW.search(stripped) or
                _RE_PIPE_START.match(stripped)):
                stats['removed_table'] += 1
                stats['removed_lines'] += 1
                continue

            # ── Categorise additional removal reasons ──
            if _RE_LATEX.search(stripped):
                stats['removed_latex'] += 1
                stats['removed_lines'] += 1
                continue
            if _RE_EMPTY_BRACKETS.match(stripped):
                stats['removed_brackets'] += 1
                stats['removed_lines'] += 1
                continue
            if _RE_WIKI_IMAGE.search(stripped):
                stats['removed_wiki_image'] += 1
                stats['removed_lines'] += 1
                continue
            if _RE_FILE_PATH.search(stripped):
                stats['removed_file_path'] += 1
                stats['removed_lines'] += 1
                continue
            if _RE_PHONETIC.search(stripped):
                stats['removed_phonetic'] += 1
                stats['removed_lines'] += 1
                continue

            # ── Clean the line ──
            cleaned = _clean_line(line)

            if cleaned is None:
                if len(stripped) < MIN_LINE_LENGTH:
                    stats['removed_short'] += 1
                else:
                    stats['removed_other'] += 1
                stats['removed_lines'] += 1
                continue

            # Track markup cleaning stats
            if '[[' in line and '[[' not in cleaned:
                stats['cleaned_wiki_links'] += 1
            if '{{' in line and '{{' not in cleaned:
                stats['cleaned_templates'] += 1
            if _RE_BULLET.match(stripped) and not _RE_BULLET.match(cleaned):
                stats['cleaned_bullets'] += 1
            if ('<ref' in line.lower() or '</ref' in line.lower()) and '<ref' not in cleaned.lower():
                stats['cleaned_refs'] += 1
            if 'http' in line and 'http' not in cleaned:
                stats['cleaned_urls'] += 1
            if '[' in line and _RE_INLINE_BRACKETS.search(line) and not _RE_INLINE_BRACKETS.search(cleaned):
                stats['cleaned_brackets'] += 1
            if '/' in line and _RE_INLINE_PHONETIC.search(line) and not _RE_INLINE_PHONETIC.search(cleaned):
                stats['cleaned_phonetic_inline'] += 1

            write_buf.append(cleaned)
            stats['kept_lines'] += 1

            # Flush buffer periodically
            if len(write_buf) >= WRITE_BUFFER:
                f_out.write('\n'.join(write_buf) + '\n')
                write_buf.clear()

            # Progress report
            if i % REPORT_EVERY == 0:
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                pct = i / total_lines * 100 if total_lines else 0
                print(
                    f"  [{i:>12,} / {total_lines:,}]  "
                    f"{pct:5.1f}%  "
                    f"kept={stats['kept_lines']:,}  "
                    f"removed={stats['removed_lines']:,}  "
                    f"({rate:,.0f} lines/s)"
                )

        # Flush remaining
        if write_buf:
            f_out.write('\n'.join(write_buf) + '\n')

    elapsed = time.time() - t0

    # ── Final report ──
    print()
    print("=" * 65)
    print(f"{'WIKIPEDIA CLEANING COMPLETE':^65}")
    print("=" * 65)
    print(f"  Total lines processed:    {stats['total_lines']:>12,}")
    print(f"  Lines kept:               {stats['kept_lines']:>12,}")
    print(f"  Lines removed:            {stats['removed_lines']:>12,}")
    print()
    print("  Removal breakdown:")
    print(f"    Empty / whitespace:     {stats['removed_empty']:>12,}")
    print(f"    Category tags:          {stats['removed_category']:>12,}")
    print(f"    Table / infobox:        {stats['removed_table']:>12,}")
    print(f"    Short fragments:        {stats['removed_short']:>12,}")
    print(f"    LaTeX / math markup:    {stats['removed_latex']:>12,}")
    print(f"    Empty brackets:         {stats['removed_brackets']:>12,}")
    print(f"    Wiki image params:      {stats['removed_wiki_image']:>12,}")
    print(f"    File paths:             {stats['removed_file_path']:>12,}")
    print(f"    Phonetic notation:      {stats['removed_phonetic']:>12,}")
    print(f"    Other (stubs/coords):   {stats['removed_other']:>12,}")
    print()
    print("  Markup cleaned (in kept lines):")
    print(f"    Wiki links stripped:    {stats['cleaned_wiki_links']:>12,}")
    print(f"    Templates removed:      {stats['cleaned_templates']:>12,}")
    print(f"    Bullet markers stripped:{stats['cleaned_bullets']:>12,}")
    print(f"    Ref tags removed:       {stats['cleaned_refs']:>12,}")
    print(f"    URLs removed:           {stats['cleaned_urls']:>12,}")
    print(f"    Brackets removed:       {stats['cleaned_brackets']:>12,}")
    print(f"    Phonetic inline rem:    {stats['cleaned_phonetic_inline']:>12,}")
    print()
    if stats['total_lines']:
        keep_pct = stats['kept_lines'] / stats['total_lines'] * 100
        print(f"  Keep rate:                {keep_pct:>11.1f}%")
    print(f"  Wall time:                {elapsed:>11.1f}s")
    if elapsed > 0:
        print(f"  Throughput:               {stats['total_lines'] / elapsed:>11,.0f} lines/s")
    print(f"\n  Output: {output_path}")
    print("=" * 65)


if __name__ == '__main__':
    main()
