import sys
import os

def normalize_malayalam_text(text):
    """
    Replaces legacy sequence (Consonant + Virama + ZWJ) with Atomic Chillus.
    Also handles ZWNJ if specific cleanup is needed.
    """
    VIRAMA = '\u0D4D'
    ZWJ = '\u200D'
    ZWNJ = '\u200C'  

    chillu_map = {
        # 1. ണ + ് + ZWJ -> ണ്‍ (Chillu NN)
        '\u0D23' + VIRAMA + ZWJ : '\u0D7A', 
        
        # 2. ന + ് + ZWJ -> ന്‍ (Chillu N)
        '\u0D28' + VIRAMA + ZWJ : '\u0D7B',

        # 3. ര + ് + ZWJ -> ര്‍ (Chillu RR)
        '\u0D30' + VIRAMA + ZWJ : '\u0D7C',

        # 4. ല + ് + ZWJ -> ല്‍ (Chillu L)
        '\u0D32' + VIRAMA + ZWJ : '\u0D7D',

        # 5. ള + ് + ZWJ -> ള്‍ (Chillu LL)
        '\u0D33' + VIRAMA + ZWJ : '\u0D7E'

    }

    # We use simple string replacement as it is faster than regex for fixed sequences
    for old_seq, new_char in chillu_map.items():
        if old_seq in text:
            text = text.replace(old_seq, new_char)

    # NOTE: In modern Unicode, [Consonant + Virama + ZWNJ] is the valid standard 
    # for "Explicit Virama" (showing the Chandrakkala explicitly).
    # However, if you want to remove ZWNJ to force ligatures (Koottaksharam), 
    
    # text = text.replace(ZWNJ, '') 

    return text

def process_file(input_filename, output_filename):
    try:
        # Open with utf-8 encoding to handle Malayalam characters correctly
        with open(input_filename, 'r', encoding='utf-8') as f_in:
            content = f_in.read()

        normalized_content = normalize_malayalam_text(content)

        with open(output_filename, 'w', encoding='utf-8') as f_out:
            f_out.write(normalized_content)
            
        print(f"Success! Normalized text written to '{output_filename}'")
        
    except FileNotFoundError:
        print(f"Error: File '{input_filename}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    process_file('sangraha_subset.txt', 'sangraha_ZWJ_removed.txt')