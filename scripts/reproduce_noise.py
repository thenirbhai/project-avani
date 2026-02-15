
import re

# Patterns to look for
patterns = [
    re.compile(r'\[\]'),
    re.compile(r'\[p\]'),
    re.compile(r'\[c\]'),
    re.compile(r'/[a-z]{2}/'),
    re.compile(r'/t~t/'),
    re.compile(r'/d~d/'),
    re.compile(r'/~~/'),
    re.compile(r'/i~i~ai~aj~ej~oj/')
]

input_file = '../data/wikipedia_mal_raw.txt'

print(f"Scanning {input_file} for noise patterns...")

try:
    with open(input_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            line = line.strip()
            for pat in patterns:
                if pat.search(line):
                    print(f"Line {i+1}: {line}")
                    break
            if i > 1000000: # Scan first 1 million lines
                break
except FileNotFoundError:
    print(f"File not found: {input_file}")
