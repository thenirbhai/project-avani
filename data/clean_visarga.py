
import re
import os

def load_valid_words(filepath):
    """
    Loads valid Visarga words from the given file.
    Assumes the first token on each line is the word.
    """
    valid_words = set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if parts:
                    # The word is the first part (e.g., "അതഃ")
                    valid_words.add(parts[0])
    except FileNotFoundError:
        print(f"Error: Dictionary file not found at {filepath}")
    return valid_words

def clean_text(text, valid_words):
    """
    Replaces Visarga ('ഃ') with ':' unless the word ending in Visarga is in valid_words.
    """
    # Regex to capture a word ending with Visarga.
    # We use \S+ to capture non-whitespace characters preceding the Visarga.
    # If using \w+, verify it captures Unicode characters correctly. 
    # \S+ is safer to capture "Labelഃ" as "Label" + "ഃ".
    # Pattern: capture the word part before Visarga, then the Visarga.
    
    pattern = re.compile(r'(\S+)(ഃ)')
    
    def replacement_func(match):
        full_match = match.group(0) # e.g., "അതഃ" or "Labelഃ"
        word_part = match.group(1) # e.g., "അത" or "Label"
        
        # Check if the full match (word with Visarga) is in our valid set
        if full_match in valid_words:
            return full_match
        else:
            # If not valid, replace Visarga with colon
            return f"{word_part}:"

    return pattern.sub(replacement_func, text)

def main():
    base_dir = r"e:\project-avani\data"
    words_file = os.path.join(base_dir, "visarga_words.txt")
    input_file = os.path.join(base_dir, "sangraha_subset.txt")
    output_file = os.path.join(base_dir, "sangraha_subset_cleaned.txt")
    
    print(f"Loading valid words from {words_file}...")
    valid_words = load_valid_words(words_file)
    print(f"Loaded {len(valid_words)} valid words.")
    
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at {input_file}")
        return

    print(f"Processing {input_file}...")
    try:
        with open(input_file, 'r', encoding='utf-8') as f_in:
            text = f_in.read()
            
        cleaned_text = clean_text(text, valid_words)
        
        with open(output_file, 'w', encoding='utf-8') as f_out:
            f_out.write(cleaned_text)
            
        print(f"Cleaned text written to {output_file}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

    # --- Verification/Test Snippet ---
    print("\n--- Running Internal Verification ---")
    test_cases = [
        ("Labelഃ Value", "Label: Value"),
        ("അതഃ ശരിയാണ്", "അതഃ ശരിയാണ്"), # Valid word
        ("Dateഃ 2023", "Date: 2023"),
        ("InvalidWordഃ", "InvalidWord:"),
        ("ശനെഃ മെല്ലെ", "ശനെഃ മെല്ലെ"), # Valid word
    ]
    
    for input_str, expected in test_cases:
        result = clean_text(input_str, valid_words)
        status = "PASS" if result == expected else f"FAIL (Got '{result}')"
        print(f"Test '{input_str}' -> '{expected}': {status}")

if __name__ == "__main__":
    main()
