"""
export_dataset.py
=================
Step 1: Download the ai4bharat/sangraha Malayalam dataset from HuggingFace
and export it to a plain text file (one document per line).

Internal newlines within a document are escaped as literal \\n so that
each line in the output file = exactly one document.

Usage (Colab):
    !pip install datasets tqdm
    %run export_dataset.py

Output:
    sangraha_raw.txt  (~X GB depending on dataset size)
"""

import time
from datasets import load_dataset
from tqdm.auto import tqdm

OUTPUT_FILE = 'wikipedia_mal_raw.txt'

def main():
    t0 = time.time()

    print("Loading dataset from HuggingFace...")
    # ds = load_dataset("ai4bharat/sangraha", data_dir="verified/mal", split="train", streaming=True)
    ds = load_dataset("omarkamali/wikipedia-monthly", "latest.ml", split="train", streaming=True)


    doc_count = 0
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for row in tqdm(ds, desc="Exporting", unit=" docs"):
            text = row.get('text', '')
            f.write(text.replace('\n', '\\n') + '\n')
            doc_count += 1

    elapsed = time.time() - t0
    print(f"\nDone! Exported {doc_count:,} documents to {OUTPUT_FILE}")
    print(f"Time: {elapsed:.1f}s")


if __name__ == '__main__':
    main()
