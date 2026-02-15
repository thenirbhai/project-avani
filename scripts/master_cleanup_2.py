"""
master_cleanup_2.py
===================
Step 2 Cleanup: Visarga Normalization + Chillu Word Normalization

Processes sv_mal_cleaned.txt (output of master_cleanup_1) with:
  1. Visarga cleanup   — replace misplaced Visarga (ഃ) followed by space/EOL with ':'
  2. Chillu word norm.  — replace consonant+virama words with atomic chillu equivalents
                          using the chillu_pairs_found.json dictionary

Memory-efficient design (runs on 16 GB RAM with 30 GB files):
  • Workers stream lines one-at-a-time (never load full chunk into memory)
  • Workers write to temp files (no large IPC return)
  • Temp files concatenated in order at the end
  • Dict loaded once per worker (~1.5 GB × N workers)

Usage:
    python master_cleanup_2.py
    python master_cleanup_2.py --input <file> --output <file> --mapping <json> --workers N
"""

import os
import re
import sys
import json
import string
import shutil
import tempfile
import argparse
import multiprocessing as mp
from time import perf_counter

# ════════════════════════════════════════════════════════════════════
# DEFAULTS
# ════════════════════════════════════════════════════════════════════

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT   = os.path.join(SCRIPT_DIR, '..', 'data', 'sv_mal_cleaned.txt')
DEFAULT_OUTPUT  = os.path.join(SCRIPT_DIR, '..', 'data', 'sv_mal_cleaned_v2.txt')
DEFAULT_MAPPING = os.path.join(SCRIPT_DIR, '..', 'data', 'chillu_pairs_found.json')

# ════════════════════════════════════════════════════════════════════
# 1. VISARGA CLEANUP
# ════════════════════════════════════════════════════════════════════

_RE_VISARGA = re.compile(r'ഃ(?=\s|$)')


def clean_visarga(text: str) -> str:
    """Replace misplaced Visarga (ഃ) with colon when followed by space/EOL."""
    return _RE_VISARGA.sub(':', text)


# ════════════════════════════════════════════════════════════════════
# 2. CHILLU WORD NORMALIZATION
# ════════════════════════════════════════════════════════════════════

# Per-worker globals (set by initializer)
_CHILLU_MAP = None
_PUNCTUATION = None


def _init_worker(mapping_file_path: str):
    """Load the mapping dict once per worker from the JSON file.

    This avoids pickling+sending the huge dict through IPC.
    Each worker loads it independently from disk, which is fast
    and avoids the pickle memory spike.
    """
    global _CHILLU_MAP, _PUNCTUATION
    _PUNCTUATION = string.punctuation + "\u2018\u2019\u201C\u201D"
    if mapping_file_path and os.path.isfile(mapping_file_path):
        with open(mapping_file_path, 'r', encoding='utf-8') as f:
            _CHILLU_MAP = json.load(f)
    else:
        _CHILLU_MAP = {}


def _normalize_word(raw_word: str) -> str:
    """Normalize a single token using the chillu mapping dictionary."""
    mapping = _CHILLU_MAP
    punct = _PUNCTUATION

    # Separate leading punctuation
    l_stripped = raw_word.lstrip(punct)
    leading_punct = raw_word[:len(raw_word) - len(l_stripped)]

    # Separate trailing punctuation
    core_word = l_stripped.rstrip(punct)
    trailing_punct = l_stripped[len(core_word):]

    # Look up core word in mapping
    if core_word in mapping:
        return f"{leading_punct}{mapping[core_word]}{trailing_punct}"

    return raw_word


def _normalize_chillu_line(line: str) -> str:
    """Split line into words, normalize each, rejoin."""
    if not _CHILLU_MAP:
        return line
    words = line.split()
    if not words:
        return line
    return ' '.join(_normalize_word(w) for w in words)


# ════════════════════════════════════════════════════════════════════
# 3. WORKER: STREAM CHUNK → TEMP FILE
# ════════════════════════════════════════════════════════════════════

def _process_chunk_to_file(args):
    """Read a byte-range chunk LINE-BY-LINE, process, write to temp file.

    Returns (temp_file_path, lines_processed).
    Memory: only 1 line in memory at a time (+ the dict).
    """
    filepath, start, end, temp_dir, chunk_idx = args

    temp_path = os.path.join(temp_dir, f"chunk_{chunk_idx:04d}.txt")
    lines_processed = 0

    with open(filepath, 'r', encoding='utf-8', errors='replace') as f_in, \
         open(temp_path, 'w', encoding='utf-8', buffering=8 * 1024 * 1024) as f_out:  # 8 MB write buffer

        # Seek to chunk start, skip partial line if not at byte 0
        if start > 0:
            f_in.buffer.seek(start)
            f_in.buffer.readline()  # discard partial line
        else:
            f_in.buffer.seek(0)

        while True:
            pos = f_in.buffer.tell()
            if pos >= end:
                break

            line = f_in.readline()
            if not line:
                break

            # Process line
            text = line.rstrip('\n\r')
            text = clean_visarga(text)
            text = _normalize_chillu_line(text)

            f_out.write(text + '\n')
            lines_processed += 1

    return (temp_path, lines_processed)


# ════════════════════════════════════════════════════════════════════
# 4. CONCATENATE TEMP FILES
# ════════════════════════════════════════════════════════════════════

def _concatenate_files(temp_paths: list[str], output_file: str):
    """Concatenate temp chunk files into the final output using binary copy."""
    with open(output_file, 'wb') as f_out:
        for temp_path in temp_paths:
            with open(temp_path, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out, length=16 * 1024 * 1024)  # 16 MB copy buffer


# ════════════════════════════════════════════════════════════════════
# 5. MAIN
# ════════════════════════════════════════════════════════════════════

def _compute_chunks(filepath: str, num_chunks: int, temp_dir: str):
    """Split file into byte-range chunks."""
    file_size = os.path.getsize(filepath)
    chunk_size = file_size // num_chunks
    chunks = []
    start = 0
    for i in range(num_chunks):
        end = start + chunk_size if i < num_chunks - 1 else file_size
        chunks.append((filepath, start, end, temp_dir, i))
        start = end
    return chunks, file_size


def main():
    parser = argparse.ArgumentParser(
        description='Master cleanup step 2: Visarga + Chillu normalization (memory-efficient)'
    )
    parser.add_argument('--input', '-i', default=DEFAULT_INPUT,
                        help='Input text file (default: sv_mal_cleaned.txt)')
    parser.add_argument('--output', '-o', default=DEFAULT_OUTPUT,
                        help='Output text file (default: sv_mal_cleaned_v2.txt)')
    parser.add_argument('--mapping', '-m', default=DEFAULT_MAPPING,
                        help='Chillu pairs JSON mapping file')
    parser.add_argument('--workers', '-w', type=int,
                        default=min(3, max(1, mp.cpu_count() - 1)),
                        help='Number of parallel workers (default: min(3, cpu_count-1))')
    args = parser.parse_args()

    input_file   = os.path.abspath(args.input)
    output_file  = os.path.abspath(args.output)
    mapping_file = os.path.abspath(args.mapping)

    # --- Validate ---
    if not os.path.isfile(input_file):
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    # --- Check mapping ---
    if os.path.isfile(mapping_file):
        map_size = os.path.getsize(mapping_file)
        print(f"Mapping file: {mapping_file} ({map_size / (1024**2):.0f} MB)")
    else:
        print(f"Warning: Mapping file not found: {mapping_file}")
        print(f"  Proceeding with Visarga cleanup only.")

    num_workers = args.workers
    file_size = os.path.getsize(input_file)
    size_gb = file_size / (1024 ** 3)

    # --- Memory estimate ---
    # Dict ~1.5 GB per worker copy + ~negligible per-line streaming
    est_mem_gb = num_workers * 1.8 + 1.0  # dict copies + OS overhead
    print(f"\n{'='*60}")
    print(f"  MASTER CLEAN 2 — Visarga + Chillu Normalization")
    print(f"{'='*60}")
    print(f"  Input   : {input_file}")
    print(f"  Output  : {output_file}")
    print(f"  Size    : {size_gb:.2f} GB")
    print(f"  Workers : {num_workers}")
    print(f"  Est. RAM: ~{est_mem_gb:.1f} GB (dict: ~1.8 GB × {num_workers} workers)")

    if est_mem_gb > 14:
        print(f"\n  ⚠️  Estimated memory ({est_mem_gb:.1f} GB) is close to 16 GB limit!")
        print(f"  Consider using --workers {max(1, num_workers - 1)}")

    print(f"{'='*60}\n")

    # --- Create temp directory ---
    temp_dir = tempfile.mkdtemp(prefix='master_clean2_')
    print(f"Temp dir: {temp_dir}")

    try:
        # --- Compute chunks ---
        chunks, _ = _compute_chunks(input_file, num_workers, temp_dir)
        print(f"Split into {len(chunks)} chunks")
        print("Processing...\n", flush=True)

        t_start = perf_counter()

        # --- Process chunks in parallel ---
        # Workers load the dict from FILE (not pickled through IPC)
        # This avoids the pickle memory spike
        ordered_results = []

        with mp.Pool(
            processes=num_workers,
            initializer=_init_worker,
            initargs=(mapping_file,)
        ) as pool:
            for i, (temp_path, lines_done) in enumerate(
                pool.imap(_process_chunk_to_file, chunks)
            ):
                ordered_results.append(temp_path)
                elapsed = perf_counter() - t_start
                done_pct = (i + 1) / len(chunks) * 100
                speed = (file_size * (i + 1) / len(chunks)) / elapsed / (1024**3) if elapsed > 0 else 0
                print(f"  Chunk {i+1}/{len(chunks)} done  |  "
                      f"Lines: {lines_done:,}  |  "
                      f"{done_pct:.0f}%  |  "
                      f"{speed:.2f} GB/s  |  "
                      f"Elapsed: {elapsed:.1f}s",
                      flush=True)

        t_process = perf_counter() - t_start
        print(f"\nProcessing done in {t_process:.1f}s")

        # --- Concatenate temp files into final output ---
        print("Concatenating output...", flush=True)
        t_concat = perf_counter()
        _concatenate_files(ordered_results, output_file)
        t_concat = perf_counter() - t_concat
        print(f"Concatenation done in {t_concat:.1f}s")

    finally:
        # --- Cleanup temp files ---
        print("Cleaning up temp files...", flush=True)
        shutil.rmtree(temp_dir, ignore_errors=True)

    t_total = perf_counter() - t_start

    # --- Final report ---
    out_size = os.path.getsize(output_file)
    print(f"\n{'='*60}")
    print(f"  {'CLEANUP COMPLETE':^56}")
    print(f"{'='*60}")
    print(f"  Input  size:            {size_gb:>13.2f} GB")
    print(f"  Output size:            {out_size / (1024**3):>13.2f} GB")
    print(f"  Wall time:              {t_total:>13.1f}s  ({t_total/60:.1f} min)")
    if t_total > 0:
        print(f"  Throughput:             {size_gb / t_total * 60:>13.1f} GB/min")
    print(f"\n  Output: {output_file}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
