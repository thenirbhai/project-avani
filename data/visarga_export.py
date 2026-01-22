import re

def extract_visarga_lines(input_file, output_file):
    # Pattern Explanation:
    # ^             : Start of the line (ensures we only check the headword)
    # \S* : Matches the characters of the word (non-whitespace)
    # [\u0D03\u0903] : Character class matching either Malayalam Visarga (ഃ) or Devanagari (ः)
    # (?=\s)        : Lookahead to ensure the next char is a space/tab (confirms end of word)
    visarga_pattern = re.compile(r'^\S*[\u0D03\u0903](?=\s)')
    
    count = 0
    print("Working...")
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            for line in infile:
                # We use match() because we only care about the beginning of the line
                if visarga_pattern.match(line):
                    outfile.write(line)
                    count += 1
                    
        print(f"Success! Processed '{input_file}'.")
        print(f"Exported {count} lines containing Visarga-ending words to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# --- Execute ---
# Ensure your dictionary file is named 'dictionary.txt' or change the name below
extract_visarga_lines('sangraha_subset_cleaned.txt', 'check_visarga.txt')