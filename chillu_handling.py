import re
import os

def rigorous_malayalam_normalization(text):
    """
    Normalizes Malayalam text by converting legacy/lazy Chillu forms 
    to modern Atomic Unicode characters while preserving ligatures.
    """
    if not isinstance(text, str):
        return ""

    # Mappings: Base Consonant -> Atomic Chillu
    chillu_map = {
        '\u0D23': '\u0D7E', # ണ -> ൺ
        '\u0D28': '\u0D7B', # ന -> ൻ
        '\u0D30': '\u0D7C', # ര -> ർ
        '\u0D32': '\u0D7D', # ല -> ൽ
        '\u0D33': '\u0D7A', # ള -> ൾ
    }

    virama = '\u0D4D'
    zwj = '\u200D'
    zwnj = '\u200C'
    malayalam_consonants = r'\u0D15-\u0D39'

    def replace_callback(match):
        base = match.group(1)
        suffix = match.group(2)
        lookahead = match.group(3)

        if zwnj in suffix:
            return base + virama
        if zwj in suffix:
            return chillu_map.get(base, base + virama)
        
        # Lazy Case: Only convert if NOT followed by another consonant (ligature)
        if lookahead and re.match(f'[{malayalam_consonants}]', lookahead):
            return base + virama
        
        return chillu_map.get(base, base + virama)

    # Regex to catch Base + Virama + optional ZWJ/ZWNJ
    pattern = r'([\u0D23\u0D28\u0D30\u0D32\u0D33\u0D15])(\u0D4D[\u200D\u200C]?)(.|$)'
    
    # Process normalization
    normalized_text = re.sub(pattern, lambda m: replace_callback(m) + (m.group(3) if m.group(3) and m.group(3) not in [zwj, zwnj] else ""), text, flags=re.DOTALL)
    
    # Remove remaining stray ZWJ/ZWNJ characters
    normalized_text = normalized_text.replace(zwj, "").replace(zwnj, "")
    return normalized_text

def process_file():
    input_filename = "sangraha_subset.txt"
    if not os.path.exists(input_filename):
        print(f"Error: The file '{input_filename}' was not found.")
        return

    output_filename = "normalized_" + input_filename

    try:
        # 1. Read the input file
        with open(input_filename, 'r', encoding='utf-8') as infile:
            content = infile.read()

        # 2. Run the normalization
        print("Normalizing Malayalam text...")
        cleaned_data = rigorous_malayalam_normalization(content)

        # 3. Write to the output file
        with open(output_filename, 'w', encoding='utf-8') as outfile:
            outfile.write(cleaned_data)

        print(f"--- Success! ---")
        print(f"Original file: {input_filename}")
        print(f"Normalized file created: {output_filename}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_file()