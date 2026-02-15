"""
Optimized Chillu Letter Extractor
==================================
Extracts unique Malayalam words containing chillu letter representations
from large text files. Optimized for 30 GB+ files using:
  - Multiprocessing (parallel chunk processing)
  - Pre-filtering (skip lines without relevant Unicode chars)
  - Chunked file reading (efficient I/O)

Usage:
    python chillu_extract_optimized.py <input_file> <output_file> [--workers N]
    python chillu_extract_optimized.py   # uses default paths
"""

import re
import os
import sys
import argparse
import mmap
import multiprocessing as mp
from functools import partial
from time import perf_counter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Characters that MUST be present in a line for it to potentially match
# Virama (0D4D) or any atomic chillu (0D7A-0D7F)
PREFILTER_CHARS = frozenset('\u0D4D\u0D7A\u0D7B\u0D7C\u0D7D\u0D7E\u0D7F')

# Compiled pattern (same logic as original)
PATTERN_LOGIC = r'([\u0D28\u0D23\u0D30\u0D33\u0D32]\u0D4D(?![നണരളല])|[\u0D7A-\u0D7F])'
MAL_RANGE = r'[\u0D00-\u0D7F]'
FULL_WORD_RE = re.compile(MAL_RANGE + r'*' + PATTERN_LOGIC + MAL_RANGE + r'*')

# Default paths (relative to script location)
DEFAULT_INPUT  = '../data/sv_mal_cleaned.txt'
DEFAULT_OUTPUT = '../data/chillu_unique_words.txt'

# ---------------------------------------------------------------------------
# Worker function (runs in each subprocess)
# ---------------------------------------------------------------------------

def _process_chunk(args):
    """Process a byte-range [start, end) of the file and return unique words."""
    filepath, start, end = args
    local_words = set()
    prefilter = PREFILTER_CHARS  # local ref for speed

    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        # Seek to start; if not at byte 0, skip partial first line
        if start > 0:
            f.buffer.seek(start)
            f.buffer.readline()  # discard partial line
        else:
            f.buffer.seek(0)

        while True:
            pos = f.buffer.tell()
            if pos >= end:
                break

            line = f.readline()
            if not line:
                break

            # Fast pre-filter: skip lines that can't possibly match
            if not prefilter.intersection(line):
                continue

            for m in FULL_WORD_RE.finditer(line):
                word = m.group()
                if word:
                    local_words.add(word)

    return local_words


# ---------------------------------------------------------------------------
# Chunk calculator
# ---------------------------------------------------------------------------

def _compute_chunks(filepath, num_chunks):
    """Split file into roughly equal byte-range chunks."""
    file_size = os.path.getsize(filepath)
    chunk_size = file_size // num_chunks
    chunks = []
    start = 0
    for i in range(num_chunks):
        end = start + chunk_size if i < num_chunks - 1 else file_size
        chunks.append((filepath, start, end))
        start = end
    return chunks, file_size


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Optimized chillu letter word extractor for large files'
    )
    parser.add_argument('input_file', nargs='?', default=DEFAULT_INPUT,
                        help='Path to input text file')
    parser.add_argument('output_file', nargs='?', default=DEFAULT_OUTPUT,
                        help='Path to output file for unique words')
    parser.add_argument('--workers', '-w', type=int,
                        default=max(1, mp.cpu_count() - 1),
                        help='Number of parallel workers (default: CPU count - 1)')
    args = parser.parse_args()

    # Resolve paths relative to script directory (for default paths)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file  = os.path.join(script_dir, args.input_file)  if not os.path.isabs(args.input_file)  else args.input_file
    output_file = os.path.join(script_dir, args.output_file) if not os.path.isabs(args.output_file) else args.output_file

    if not os.path.isfile(input_file):
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    num_workers = args.workers
    print(f"Input  : {input_file}")
    print(f"Output : {output_file}")
    print(f"Workers: {num_workers}")

    t0 = perf_counter()

    # Split file into chunks
    chunks, file_size = _compute_chunks(input_file, num_workers)
    size_gb = file_size / (1024 ** 3)
    print(f"File size: {size_gb:.2f} GB  |  Chunks: {len(chunks)}")
    print("Processing...\n")

    # Process chunks in parallel
    all_words = set()
    with mp.Pool(processes=num_workers) as pool:
        for i, result_set in enumerate(pool.imap_unordered(_process_chunk, chunks)):
            all_words.update(result_set)
            elapsed = perf_counter() - t0
            print(f"  Chunk {i+1}/{len(chunks)} done  |  "
                  f"Unique words so far: {len(all_words):,}  |  "
                  f"Elapsed: {elapsed:.1f}s")

    t_process = perf_counter() - t0
    print(f"\nExtraction complete in {t_process:.1f}s")
    print(f"Total unique words: {len(all_words):,}")

    # Sort and write output
    print("Sorting and writing output...")
    sorted_words = sorted(all_words)
    with open(output_file, 'w', encoding='utf-8') as outfile:
        outfile.write('\n'.join(sorted_words) + '\n')

    t_total = perf_counter() - t0
    print(f"Done! Total time: {t_total:.1f}s")
    print(f"Throughput: {size_gb / t_total * 60:.1f} GB/min")


if __name__ == '__main__':
    main()
