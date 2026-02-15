"""
Text File Metrics Analyzer
============================
Computes comprehensive metrics for a text file including word counts,
character statistics, Malayalam-specific analysis, and more.
Optimized for large files with streaming line-by-line processing.

Usage:
    python text_metrics.py <input_file>
    python text_metrics.py                  # uses default path
"""

import os
import sys
import re
import argparse
import collections
from time import perf_counter

# ---------------------------------------------------------------------------
# Unicode ranges
# ---------------------------------------------------------------------------
MAL_RANGE = re.compile(r'[\u0D00-\u0D7F]')
ENG_RANGE = re.compile(r'[A-Za-z]')
DIGIT_RANGE = re.compile(r'[0-9\u0D66-\u0D6F]')  # ASCII + Malayalam digits
PUNCT_RANGE = re.compile(r'[^\w\s]', re.UNICODE)
WHITESPACE_RE = re.compile(r'\s+')

# Word tokenizer: sequences of non-whitespace
WORD_RE = re.compile(r'\S+')

# Malayalam word: contains at least one Malayalam character
MAL_WORD_RE = re.compile(r'[\u0D00-\u0D7F]')

DEFAULT_INPUT = '../data/sv_mal_cleaned.txt'


def format_size(size_bytes):
    """Format bytes into human-readable size."""
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def analyze_file(input_file):
    file_size = os.path.getsize(input_file)

    # Counters
    total_lines = 0
    blank_lines = 0
    total_chars = 0
    mal_chars = 0
    eng_chars = 0
    digit_chars = 0
    punct_chars = 0
    whitespace_chars = 0

    total_words = 0
    mal_words = 0
    word_freq = collections.Counter()
    word_lengths = collections.Counter()
    unique_words = set()

    longest_line_len = 0
    shortest_line_len = float('inf')
    longest_word = ""
    shortest_word_len = float('inf')

    print(f"Analyzing: {input_file}")
    print(f"File size: {format_size(file_size)}")
    print("Processing...\n")

    t0 = perf_counter()
    bytes_read = 0
    last_report = t0

    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            total_lines += 1
            line_stripped = line.rstrip('\n\r')
            line_len = len(line_stripped)

            # Line metrics
            if not line_stripped.strip():
                blank_lines += 1

            if line_len > longest_line_len:
                longest_line_len = line_len
            if line_len < shortest_line_len and line_stripped.strip():
                shortest_line_len = line_len

            # Character metrics
            total_chars += line_len
            mal_chars += len(MAL_RANGE.findall(line_stripped))
            eng_chars += len(ENG_RANGE.findall(line_stripped))
            digit_chars += len(DIGIT_RANGE.findall(line_stripped))
            punct_chars += len(PUNCT_RANGE.findall(line_stripped))
            whitespace_chars += len(line_stripped) - len(line_stripped.replace(' ', '').replace('\t', ''))

            # Word metrics
            words = WORD_RE.findall(line_stripped)
            total_words += len(words)

            for w in words:
                unique_words.add(w)
                word_freq[w] += 1
                wlen = len(w)
                word_lengths[wlen] += 1

                if MAL_WORD_RE.search(w):
                    mal_words += 1

                if wlen > len(longest_word):
                    longest_word = w
                if wlen < shortest_word_len:
                    shortest_word_len = wlen

            # Progress reporting (every 2 seconds)
            bytes_read += len(line.encode('utf-8', errors='replace'))
            now = perf_counter()
            if now - last_report > 2.0:
                pct = (bytes_read / file_size * 100) if file_size > 0 else 0
                speed = bytes_read / (now - t0) / (1024 * 1024)
                print(f"  Progress: {pct:.1f}%  |  Lines: {total_lines:,}  |  "
                      f"Speed: {speed:.1f} MB/s", end='\r', flush=True)
                last_report = now

    elapsed = perf_counter() - t0

    # Compute derived stats
    num_unique = len(unique_words)
    avg_word_len = sum(l * c for l, c in word_lengths.items()) / total_words if total_words else 0
    avg_words_per_line = total_words / total_lines if total_lines else 0
    non_blank_lines = total_lines - blank_lines
    type_token_ratio = num_unique / total_words if total_words else 0

    # Top N most frequent words
    top_n = 20
    most_common = word_freq.most_common(top_n)

    # Word length distribution (top buckets)
    len_dist = sorted(word_lengths.items())

    # -----------------------------------------------------------------------
    # Print Report
    # -----------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("               TEXT FILE METRICS REPORT")
    print("=" * 60)

    print(f"\n📁 FILE INFO")
    print(f"  {'File':<30} {os.path.basename(input_file)}")
    print(f"  {'Size':<30} {format_size(file_size)}")
    print(f"  {'Analysis time':<30} {elapsed:.1f}s")
    if elapsed > 0:
        print(f"  {'Throughput':<30} {file_size / elapsed / (1024**2):.1f} MB/s")

    print(f"\n📝 LINE METRICS")
    print(f"  {'Total lines':<30} {total_lines:,}")
    print(f"  {'Non-blank lines':<30} {non_blank_lines:,}")
    print(f"  {'Blank lines':<30} {blank_lines:,}")
    print(f"  {'Longest line (chars)':<30} {longest_line_len:,}")
    if shortest_line_len != float('inf'):
        print(f"  {'Shortest non-blank line':<30} {shortest_line_len:,}")
    print(f"  {'Avg words per line':<30} {avg_words_per_line:.1f}")

    print(f"\n📊 WORD METRICS")
    print(f"  {'Total words':<30} {total_words:,}")
    print(f"  {'Unique words':<30} {num_unique:,}")
    print(f"  {'Type-Token Ratio (TTR)':<30} {type_token_ratio:.4f}")
    print(f"  {'Malayalam words':<30} {mal_words:,}")
    if total_words:
        print(f"  {'Malayalam word %':<30} {mal_words/total_words*100:.1f}%")
    print(f"  {'Avg word length':<30} {avg_word_len:.1f} chars")
    print(f"  {'Longest word':<30} {longest_word[:50]}{'...' if len(longest_word)>50 else ''} ({len(longest_word)} chars)")

    print(f"\n🔤 CHARACTER METRICS")
    print(f"  {'Total characters':<30} {total_chars:,}")
    print(f"  {'Malayalam characters':<30} {mal_chars:,}  ({mal_chars/total_chars*100:.1f}%)" if total_chars else "")
    print(f"  {'English characters':<30} {eng_chars:,}  ({eng_chars/total_chars*100:.1f}%)" if total_chars else "")
    print(f"  {'Digit characters':<30} {digit_chars:,}  ({digit_chars/total_chars*100:.1f}%)" if total_chars else "")
    print(f"  {'Punctuation characters':<30} {punct_chars:,}  ({punct_chars/total_chars*100:.1f}%)" if total_chars else "")
    print(f"  {'Whitespace characters':<30} {whitespace_chars:,}  ({whitespace_chars/total_chars*100:.1f}%)" if total_chars else "")

    print(f"\n🏆 TOP {top_n} MOST FREQUENT WORDS")
    print(f"  {'Rank':<6} {'Word':<40} {'Count':>10} {'%':>8}")
    print(f"  {'-'*6} {'-'*40} {'-'*10} {'-'*8}")
    for rank, (word, count) in enumerate(most_common, 1):
        display_word = word[:38] + '..' if len(word) > 40 else word
        pct = count / total_words * 100 if total_words else 0
        print(f"  {rank:<6} {display_word:<40} {count:>10,} {pct:>7.2f}%")

    print(f"\n📏 WORD LENGTH DISTRIBUTION")
    print(f"  {'Length':<10} {'Count':>12} {'%':>8}  Bar")
    print(f"  {'-'*10} {'-'*12} {'-'*8}  {'-'*30}")
    max_count = max(c for _, c in len_dist) if len_dist else 1
    for length, count in len_dist[:25]:  # show up to length 25
        pct = count / total_words * 100 if total_words else 0
        bar_len = int(count / max_count * 30)
        print(f"  {length:<10} {count:>12,} {pct:>7.2f}%  {'█' * bar_len}")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Text file metrics analyzer')
    parser.add_argument('input_file', nargs='?', default=DEFAULT_INPUT,
                        help='Path to input text file')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, args.input_file) if not os.path.isabs(args.input_file) else args.input_file

    if not os.path.isfile(input_file):
        print(f"Error: File not found: {input_file}")
        sys.exit(1)

    analyze_file(input_file)


if __name__ == '__main__':
    main()
