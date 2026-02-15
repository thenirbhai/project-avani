"""
remove_boilerplate.py
---------------------
Removes boilerplate / web-scraped noise from Malayalam text datasets.

Strategy:
  1. Keyword matching   – lines containing any trigger keyword are removed.
  2. Regex matching     – structural patterns (copyright, dates, nav links).
  3. Heuristic filters  – lines that are too short, mostly English, or
                          dominated by pipes / special chars.

Usage:
    python scripts/remove_boilerplate.py

Outputs:
    data/sangraha_subset_clean.txt   – cleaned text
    data/sangraha_subset_removed.txt – lines that were removed (for review)
"""

import re
import os
import sys

# ──────────────────────────────────────────────
# 1. KEYWORD TRIGGERS (case-insensitive check)
# ──────────────────────────────────────────────
BOILERPLATE_KEYWORDS = [
    # Copyright / Legal
    "©", "Copyright", "All rights reserved", "Terms of Use",
    "Privacy Policy", "Disclaimer", "cookie policy",

    # Navigation / CTA
    "Also Read", "Don't Miss", "Don't Miss", "Read More", "Click Here",
    "Read Also", "READ ALSO",
    "Subscribe", "Sign Up", "Log In", "Share this",
    "Follow us", "Join us", "See also", "Previous:", "Next:",
    "Load More", "Show More", "View More",

    # NOTE: Social media names (Facebook, Twitter, ഫേസ്ബുക്ക്, etc.)
    # are NOT included here because they appear frequently in legitimate
    # Malayalam news articles discussing these platforms.

    # Timestamps / Edition labels (English boilerplate)
    "Last Modified", "Last Updated", "English Edition",
    "Published:", "Updated:",

    # Specific recurring boilerplate lines from marunadan site
    "മറുനാടൻ ടിവിയുടെ ഫേസ്ബുക്ക് പേജ് ഹാക്ക് ചെയ്തു",
    "ഷാജൻ സ്കറിയയുടെ വീഡിയോ കാണാം",
    'കൂടുതൽ വായിക്കുക', 'തുടർന്ന് വായിക്കുക', 
    'സബ്സ്ക്രൈബ് ചെയ്യുക', 'ഷെയർ ചെയ്യുക', 'കമന്റ് ചെയ്യുക', 
    'ലൈക്ക് ചെയ്യുക', 'സെർച്ച്'

    # Ads / Promotions
    "Advertisement", "Sponsored", "Ad ",

    # Metadata
    "Keywords:", "Top-Headlines",

    # Web UI boilerplate
    "Begin typing your search above",
    "Your Comment Added Successfully",
    "consectetur adipiscing elit",  # lorem ipsum placeholder text
    "Save my name, email, and website",
    "Press CTRL+M to toggle",
    "Download the Fanport app",
]

# ──────────────────────────────────────────────
# 2. REGEX PATTERNS
# ──────────────────────────────────────────────
BOILERPLATE_PATTERNS = [
    # Copyright notice:  © ... 2018   or  Copyright ... 2020
    r"©.*\d{4}",
    r"Copyright.*\d{4}",

    # "All rights reserved" with optional surrounding text
    r"All\s+rights\s+reserved",

    # Category tags like "- News ...", "- Technology ..."
    r"^-\s*(News|Technology|Sports|Entertainment|Business|World|National|India)\s",

    # Lines that are only dashes, pipes, or whitespace  (separator lines)
    r"^\s*[\-\|•·=]{3,}\s*$",

    # Date-only lines:  01Dec2021, Monday June 17 2019, etc.
    r"^\s*\|?\s*\d{1,2}\s*[A-Za-z]+\s*\d{4}\s*$",

    # English day-date patterns at start of line
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*,?\s+\d",

    # URLs (full http/https links)
    r"https?://\S+",

    # Archived at Wayback Machine references
    r"Archived\s+\d{4}-\d{2}-\d{2}\s+at\s+the\s+Wayback\s+Machine",

    # Lines that look like pipe-separated tags / metadata
    # e.g. "tag1| tag2| tag3"  (3+ pipes in a line)
    r"^[^|]*\|[^|]*\|[^|]*\|",

    # Lines starting with "- " followed by a very long semicolon-separated list
    # (clickbait headline compilations)
    r"^-\s+.+;\s+.+;\s+.+;\s+.+;\s+",
    
    # Phone Numbers
    r'\+\d{1,4}[\s.-]?\d{1,4}[\s.-]?\d{1,4}[\s.-]?\d{1,9}'
]

# Pre-compile regex patterns
COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in BOILERPLATE_PATTERNS]

# ──────────────────────────────────────────────
# 3. HEURISTIC FILTERS
# ──────────────────────────────────────────────

# Malayalam Unicode range: U+0D00 – U+0D7F
_ML_RANGE = re.compile(r"[\u0D00-\u0D7F]")
_ALPHA = re.compile(r"[A-Za-z\u0D00-\u0D7F]")


def _malayalam_ratio(line: str) -> float:
    """Return the fraction of alphabetic chars that are Malayalam."""
    alpha_chars = _ALPHA.findall(line)
    if not alpha_chars:
        return 0.0
    ml_chars = _ML_RANGE.findall(line)
    return len(ml_chars) / len(alpha_chars)


def is_heuristic_boilerplate(line: str) -> bool:
    """
    Catch remaining noise via heuristics:
      - Lines shorter than 5 chars (excluding whitespace)
      - Lines with < 20% Malayalam characters (mostly English/noise)
        UNLESS they are very short (≤ 30 chars) – those might be legitimate
        short Malayalam phrases with some English mixed in
    """
    stripped = line.strip()

    # Very short lines are noise (but not empty – those are handled separately)
    if 0 < len(stripped) < 5:
        return True

    # Lines with very low Malayalam content (mostly English boilerplate)
    # Only apply this to longer lines to avoid false-positiving short phrases
    if len(stripped) > 30 and _malayalam_ratio(stripped) < 0.15:
        return True

    return False


# ──────────────────────────────────────────────
# 4. MAIN CLASSIFICATION FUNCTION
# ──────────────────────────────────────────────

def classify_line(line: str) -> str:
    """
    Returns:
      "keep"              – line is good content
      "remove_keyword"    – matched a keyword trigger
      "remove_regex"      – matched a regex pattern
      "remove_heuristic"  – caught by heuristic filters
      "remove_empty"      – blank / whitespace-only line
    """
    stripped = line.strip()

    if not stripped:
        return "remove_empty"

    # Keyword check (case-insensitive)
    lower = stripped.lower()
    for kw in BOILERPLATE_KEYWORDS:
        if kw.lower() in lower:
            return "remove_keyword"

    # Regex check
    for pat in COMPILED_PATTERNS:
        if pat.search(stripped):
            return "remove_regex"

    # Heuristic check
    if is_heuristic_boilerplate(stripped):
        return "remove_heuristic"

    return "keep"


# ──────────────────────────────────────────────
# 5. DRIVER
# ──────────────────────────────────────────────

def clean_file(input_path: str, output_clean: str, output_removed: str):
    counts = {
        "keep": 0,
        "remove_keyword": 0,
        "remove_regex": 0,
        "remove_heuristic": 0,
        "remove_empty": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_clean, "w", encoding="utf-8") as fout_clean, \
         open(output_removed, "w", encoding="utf-8") as fout_removed:

        for line_no, line in enumerate(fin, start=1):
            label = classify_line(line)
            counts[label] += 1

            if label == "keep":
                fout_clean.write(line)
            else:
                # Write removed line with reason + line number for review
                fout_removed.write(f"[{label}] L{line_no}: {line}")

    return counts


def main():
    # Resolve paths relative to project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    input_path = os.path.join(project_root, "data", "input.txt")
    output_clean = os.path.join(project_root, "data", "output.txt")
    output_removed = os.path.join(project_root, "data", "log.txt")

    if not os.path.exists(input_path):
        print(f"ERROR: Input file not found: {input_path}")
        sys.exit(1)

    print(f"Input:   {input_path}")
    print(f"Clean:   {output_clean}")
    print(f"Removed: {output_removed}")
    print()

    counts = clean_file(input_path, output_clean, output_removed)

    total = sum(counts.values())
    removed = total - counts["keep"]
    print("=" * 50)
    print(f"Total lines:         {total:>7,}")
    print(f"Kept:                {counts['keep']:>7,}")
    print(f"Removed (total):     {removed:>7,}")
    print(f"  - keyword match:   {counts['remove_keyword']:>7,}")
    print(f"  - regex match:     {counts['remove_regex']:>7,}")
    print(f"  - heuristic:       {counts['remove_heuristic']:>7,}")
    print(f"  - empty lines:     {counts['remove_empty']:>7,}")
    print("=" * 50)
    print(f"\nRemoval rate: {removed / total * 100:.1f}%")
    print(f"\nReview removed lines in: {output_removed}")


if __name__ == "__main__":
    main()
