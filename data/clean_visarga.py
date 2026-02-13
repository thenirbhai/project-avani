
import re
import os

def clean_text(text):
    """
    Replaces Visarga ('ഃ') with ':' only when it is followed by a space.
    This targets misplaced Visarga characters that were originally colons,
    while preserving legitimate Visarga usage within words (e.g., ദുഃഖം).
    """
    # Pattern: Visarga followed by a space (or end of string)
    # We replace only the Visarga, keeping the space intact.
    pattern = re.compile(r'ഃ(?=\s|$)')
    
    return pattern.sub(':', text)

def main():
    base_dir = r"e:\project-avani\data"
    input_file = os.path.join(base_dir, "input.txt")
    output_file = os.path.join(base_dir, "output.txt")
    
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at {input_file}")
        return

    print(f"Processing {input_file}...")
    try:
        with open(input_file, 'r', encoding='utf-8') as f_in:
            text = f_in.read()
            
        cleaned_text = clean_text(text)
        
        with open(output_file, 'w', encoding='utf-8') as f_out:
            f_out.write(cleaned_text)
            
        print(f"Cleaned text written to {output_file}")
        
    except Exception as e:
        print(f"An error occurred: {e}")

    # --- Verification/Test Snippet ---
    print("\n--- Running Internal Verification ---")
    test_cases = [
        ("Labelഃ Value", "Label: Value"),         # Visarga + space -> colon
        ("ദുഃഖം", "ദുഃഖം"),                       # Visarga inside word -> kept
        ("Dateഃ 2023", "Date: 2023"),              # Visarga + space -> colon
        ("അതഃ ശരിയാണ്", "അത: ശരിയാണ്"),          # Visarga + space -> colon
        ("ദുഃഖിതൻ വന്നു", "ദുഃഖിതൻ വന്നു"),      # Visarga inside word -> kept
        ("endഃ", "end:"),                          # Visarga at end of string -> colon
    ]
    
    for input_str, expected in test_cases:
        result = clean_text(input_str)
        status = "PASS" if result == expected else f"FAIL (Got '{result}')"
        print(f"Test '{input_str}' -> '{expected}': {status}")

if __name__ == "__main__":
    main()
