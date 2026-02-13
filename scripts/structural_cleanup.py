"""
structural_cleanup.py
---------------------
Removes structural inconsistencies from Malayalam text datasets:
  - Out-of-scope characters (foreign scripts, emojis, decorative symbols)
  - Control characters and zero-width spaces (except ZWJ/ZWNJ)
  - Repeated punctuation (... → ., !!! → !, etc.)
  - Excessive whitespace (multiple spaces, blank lines)
  - Lines that become empty after cleanup

NOTE: ZWJ/ZWNJ normalization is handled separately by ZWJ_normalization.py.
      This script leaves ZWJ (U+200D) and ZWNJ (U+200C) untouched.

Usage:
    python scripts/structural_cleanup.py

Input:  data/sangraha_subset_clean.txt  (boilerplate-removed)
Output: data/sangraha_subset_structural_clean.txt
"""

import re
import os
import sys
import unicodedata

# ──────────────────────────────────────────────
# 1. ALLOWED CHARACTER RANGES
# ──────────────────────────────────────────────
# Everything NOT in this set will be stripped (except ZWJ/ZWNJ which are preserved).

# Malayalam block: U+0D00–U+0D7F
# Basic Latin:    U+0020–U+007E  (printable ASCII: letters, digits, punctuation)
# ZWJ / ZWNJ:    U+200D, U+200C  (preserved for Malayalam conjuncts)
# Indian Rupee:   U+20B9  (₹ — common in news text)

# We also allow a small set of useful Unicode punctuation:
ALLOWED_EXTRA = set([
    '\u00B0',  # ° degree sign
    '\u00A9',  # © copyright sign
    '\u00AE',  # ® registered sign
    '\u00BD',  # ½ vulgar fraction one half
    '\u00BC',  # ¼ vulgar fraction one quarter
    '\u20B9',  # ₹ Indian Rupee sign
    '\u2026',  # … horizontal ellipsis (will be collapsed to . later)
    '\u200D',  # ZWJ — preserved (handled by separate script)
    '\u200C',  # ZWNJ — preserved (handled by separate script)
])

def is_allowed_char(ch):
    """Return True if the character should be kept."""
    cp = ord(ch)

    # Malayalam block U+0D00–U+0D7F
    if 0x0D00 <= cp <= 0x0D7F:
        return True

    # Printable ASCII (space through tilde)
    if 0x0020 <= cp <= 0x007E:
        return True

    # Tab, newline, carriage return
    if ch in ('\t', '\n', '\r'):
        return True

    # Explicitly allowed extras
    if ch in ALLOWED_EXTRA:
        return True

    return False


# ──────────────────────────────────────────────
# 2. REPEATED PUNCTUATION PATTERNS
# ──────────────────────────────────────────────
# Collapse 2+ consecutive identical punctuation marks to a single one
REPEATED_PUNCT = re.compile(r'([.!?,;:\-*#])\1+')

# Collapse horizontal ellipsis (…) and multi-dots to single period
ELLIPSIS_PATTERN = re.compile(r'[.…]{2,}')

# Asterisks (often used as separators: *** or * * *)
ASTERISK_LINE = re.compile(r'^[\s*]+$')  # lines that are only asterisks/spaces

# ──────────────────────────────────────────────
# 3. WHITESPACE NORMALIZATION
# ──────────────────────────────────────────────
# Multiple spaces/tabs → single space (within a line)
MULTI_SPACE = re.compile(r'[ \t]{2,}')

# Multiple blank lines → single blank line
MULTI_BLANK_LINES = re.compile(r'\n{3,}')

# ──────────────────────────────────────────────
# 3b. STRAY VOWEL SIGN REMOVAL
# ──────────────────────────────────────────────
# Malayalam dependent vowel signs (matras) U+0D3E–U+0D4D and AU length mark U+0D57.
# These must follow a consonant (U+0D15–U+0D39), a chillu (U+0D7A–U+0D7F),
# or another valid base. A vowel sign that appears at the start of text, after
# whitespace, after punctuation, or after another vowel sign is "stray" and invalid.

MALAYALAM_VOWEL_SIGNS = set(chr(cp) for cp in range(0x0D3E, 0x0D4E))  # ാ ി ീ ു ൂ ൃ ൄ  െ േ ൈ ൊ ോ ൌ ്
MALAYALAM_VOWEL_SIGNS.add('\u0D57')  # ൗ AU length mark

# Valid bases that may precede a vowel sign:
#   - Consonants: U+0D15–U+0D39
#   - Chillus:    U+0D7A–U+0D7F
#   - Vowel signs themselves (stacked signs, e.g. െ + ാ → ൊ in NFC edge cases)
MALAYALAM_VALID_BASES = (
    set(chr(cp) for cp in range(0x0D15, 0x0D3A))   # consonants
    | set(chr(cp) for cp in range(0x0D7A, 0x0D80))  # chillus
    | MALAYALAM_VOWEL_SIGNS                          # sign after sign (rare but valid)
)


def remove_stray_vowel_signs(text):
    """Remove Malayalam vowel signs that lack a valid consonant/chillu base.

    Returns (cleaned_text, count_of_removed_signs).
    """
    removed = 0
    result = []
    prev = None
    for ch in text:
        if ch in MALAYALAM_VOWEL_SIGNS:
            if prev is None or prev not in MALAYALAM_VALID_BASES:
                removed += 1
                continue  # drop this stray sign
        result.append(ch)
        prev = ch
    return ''.join(result), removed


# ──────────────────────────────────────────────
# 4. MAIN PROCESSING
# ──────────────────────────────────────────────

def clean_text(text):
    """Run all structural cleanup steps on the full text."""
    stats = {
        'chars_before': len(text),
        'literal_newlines_replaced': 0,
        'foreign_script_removed': 0,
        'control_chars_removed': 0,
        'emoji_removed': 0,
        'decorative_removed': 0,
        'stray_vowel_signs_removed': 0,
        'total_chars_removed': 0,
    }

    # Step 1: Unicode NFC normalization
    text = unicodedata.normalize('NFC', text)

    # Step 2: Replace literal \n (backslash + n) with actual newline
    literal_n_count = text.count('\\n')
    text = text.replace('\\n', '\n')
    stats['literal_newlines_replaced'] = literal_n_count

    # Step 3: Remove ZWNBS / BOM  (U+FEFF)
    zwnbs_count = text.count('\ufeff')
    text = text.replace('\ufeff', '')
    stats['control_chars_removed'] += zwnbs_count

    # Step 4: Character-level filtering
    # Walk through each character and keep only allowed ones
    cleaned_chars = []
    for ch in text:
        if is_allowed_char(ch):
            cleaned_chars.append(ch)
        else:
            # Classify what we're removing for stats
            cp = ord(ch)
            name = unicodedata.category(ch)

            if name.startswith('C'):  # Control, format, surrogate, private use
                stats['control_chars_removed'] += 1
            elif name.startswith('So') or name.startswith('Sk'):  # Symbols
                stats['decorative_removed'] += 1
            else:
                stats['foreign_script_removed'] += 1

    text = ''.join(cleaned_chars)

    # Step 5: Remove stray vowel signs (matras without a valid base)
    text, stray_count = remove_stray_vowel_signs(text)
    stats['stray_vowel_signs_removed'] = stray_count

    # Step 6: Collapse repeated punctuation
    text = ELLIPSIS_PATTERN.sub('.', text)
    text = REPEATED_PUNCT.sub(r'\1', text)

    # Step 7: Normalize whitespace within lines
    text = MULTI_SPACE.sub(' ', text)

    # Step 8: Process line by line — strip, drop empties, drop asterisk-only lines
    lines = text.split('\n')
    final_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if ASTERISK_LINE.match(line):
            continue
        final_lines.append(line)

    text = '\n'.join(final_lines)

    # Step 9: Collapse multiple consecutive blank lines (shouldn't remain, but safety)
    text = MULTI_BLANK_LINES.sub('\n\n', text)

    stats['chars_after'] = len(text)
    stats['total_chars_removed'] = stats['chars_before'] - stats['chars_after']

    return text, stats, len(lines), len(final_lines)


# ──────────────────────────────────────────────
# 5. DRIVER
# ──────────────────────────────────────────────

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    input_path = os.path.join(project_root, "data", "input.txt")
    output_path = os.path.join(project_root, "data", "output.txt")

    if not os.path.exists(input_path):
        print(f"ERROR: Input file not found: {input_path}")
        sys.exit(1)

    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print()

    # Read entire file
    with open(input_path, 'r', encoding='utf-8') as f:
        text = f.read()

    cleaned_text, stats, lines_before, lines_after = clean_text(text)

    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(cleaned_text)

    # Print report
    size_before = os.path.getsize(input_path)
    size_after = len(cleaned_text.encode('utf-8'))
    print("=" * 55)
    print(f"{'STRUCTURAL CLEANUP REPORT':^55}")
    print("=" * 55)
    print(f"  Characters before:        {stats['chars_before']:>12,}")
    print(f"  Characters after:         {stats['chars_after']:>12,}")
    print(f"  Total chars removed:      {stats['total_chars_removed']:>12,}")
    print(f"    - Literal \\n replaced:  {stats['literal_newlines_replaced']:>12,}")
    print(f"    - Foreign scripts:      {stats['foreign_script_removed']:>12,}")
    print(f"    - Control characters:   {stats['control_chars_removed']:>12,}")
    print(f"    - Decorative symbols:   {stats['decorative_removed']:>12,}")
    print(f"    - Stray vowel signs:    {stats['stray_vowel_signs_removed']:>12,}")
    print()
    print(f"  Lines before:             {lines_before:>12,}")
    print(f"  Lines after:              {lines_after:>12,}")
    print(f"  Lines removed:            {lines_before - lines_after:>12,}")
    print()
    print(f"  File size before:         {size_before:>12,} bytes")
    print(f"  File size after:          {size_after:>12,} bytes")
    print(f"  Size reduction:           {(size_before - size_after) / size_before * 100:>11.1f}%")
    print("=" * 55)
    print(f"\nCleaned file: {output_path}")


if __name__ == "__main__":
    main()
