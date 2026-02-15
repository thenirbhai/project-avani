
import os
import re

INPUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'wikipedia_mal_raw.txt')

NOISE_PATTERNS = [
    (re.compile(r'\[[pc]?\]'), "Brackets [] [p] [c]"),
    (re.compile(r'/(?:[a-zA-Z~]+)/'), "Phonetic /.../")
]

def count_noise():
    print(f"Scanning {INPUT_FILE}...")
    counts = {name: 0 for _, name in NOISE_PATTERNS}
    
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                for pat, name in NOISE_PATTERNS:
                    if pat.search(line):
                        counts[name] += len(pat.findall(line))
        
        print("-" * 30)
        print("Noise Patterns Found:")
        for name, count in counts.items():
            print(f"{name}: {count}")
        print("-" * 30)
        
    except FileNotFoundError:
        print("File not found.")

if __name__ == "__main__":
    count_noise()
