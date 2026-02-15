import re
import json
import sys
import os

with open('debug_start.txt', 'w') as f:
    f.write("Script started\n")

# Flush stdout to ensure real-time logging
sys.stdout.reconfigure(line_buffering=True)

def find_chillu_pairs(input_file, output_file):
    # Mapping of Consonant + Virama to Atomic Chillu
    # Based on standard Malayalam unicode mapping
    
    mapping = {
        '\u0D23\u0D4D': '\u0D7A', # BILL
        '\u0D28\u0D4D': '\u0D7B', # CHILLU N
        '\u0D30\u0D4D': '\u0D7C', # CHILLU RR
        '\u0D32\u0D4D': '\u0D7D', # CHILLU L
        '\u0D33\u0D4D': '\u0D7E', # CHILLU LL
        '\u0D15\u0D4D': '\u0D7F', # CHILLU K
    }

    # Load all unique words into a set
    print(f"Loading words from {input_file}...", flush=True)
    with open(input_file, 'r', encoding='utf-8') as f:
        # Strip whitespace and ignore empty lines
        unique_words = set(line.strip() for line in f if line.strip())
    
    print(f"Loaded {len(unique_words)} unique words.", flush=True)

    pairs = {}
    
    # Pattern to match any of the composed sequences
    pattern = re.compile('|'.join(re.escape(k) for k in sorted(mapping.keys(), key=len, reverse=True)))

    def replace_func(match):
        return mapping[match.group(0)]

    print("Searching for pairs...", flush=True)
    count = 0
    processed = 0
    total = len(unique_words)
    
    for word in unique_words:
        processed += 1
        if processed % 1000 == 0:
             print(f"Processed {processed}/{total} words...", end='\r')

        # Check if word has any composed sequence
        if any(k in word for k in mapping):
            # Create the atomic version
            atomic_word = pattern.sub(replace_func, word)
            
            # If the atomic version is different and exists in the original dataset
            if atomic_word != word and atomic_word in unique_words:
                pairs[word] = atomic_word
                count += 1

    print(f"\nFound {count} pairs.")
    
    # Save to JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(pairs, f, ensure_ascii=False, indent=4)
    print(f"Saved pairs to {output_file}")

if __name__ == "__main__":
    find_chillu_pairs('../data/chillu_unique_words.txt', '../data/chillu_pairs_found.json')
