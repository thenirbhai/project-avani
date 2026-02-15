import re

def extract_unique_malayalam_words(input_file, output_file):

    pattern_logic = r'([\u0D28\u0D23\u0D30\u0D33\u0D32]\u0D4D(?![നണരളല])|[\u0D7A-\u0D7F])'

    mal_range = r'[\u0D00-\u0D7F]'

    full_word_pattern = re.compile(mal_range + r'*' + pattern_logic + mal_range + r'*')
    
    unique_words = set()

    print("Starting extraction and deduplication...")
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        for line in infile:
            for match in full_word_pattern.finditer(line):
                word = match.group()
                if word:
                    unique_words.add(word)

    print(f"Found {len(unique_words)} unique words. Writing to file...")
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for word in sorted(unique_words):
            outfile.write(word + '\n')

extract_unique_malayalam_words('../data/sv_mal_cleaned.txt', '../data/chillu_unique_words.txt')
print("Complete!")