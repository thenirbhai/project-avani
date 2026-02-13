import json
import string

class MalayalamWordNormalizer:
    def __init__(self, mapping_file):
        self.mapping = self._load_mapping(mapping_file)
        # Create a translation table to remove punctuation for checking
        self.punctuation = string.punctuation + "‘’“”" # Standard + Smart quotes

    def _load_mapping(self, filepath):
        print(f"Loading mapping from {filepath}...")
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading file: {e}")
            return {}

    def normalize_word(self, raw_word):
        """
        Takes a single token (e.g., "വാഹനങ്ങള്,"), separates the punctuation,
        checks the core word in the dictionary, and reconstructs it.
        """
        # 1. Identify leading and trailing punctuation
        l_stripped = raw_word.lstrip(self.punctuation)
        leading_punct = raw_word[:len(raw_word) - len(l_stripped)]
        
        core_word = l_stripped.rstrip(self.punctuation)
        trailing_punct = l_stripped[len(core_word):]

        # 2. Check if the clean word exists in our mapping
        # We generally check the core word, but if the word was empty (just punctuation), skip
        if core_word in self.mapping:
            corrected_core = self.mapping[core_word]
            return f"{leading_punct}{corrected_core}{trailing_punct}"
        
        # 3. If not found, return original raw word
        return raw_word

    def normalize_sentence(self, text):
        """
        Splits text by whitespace, processes each word, and joins them back.
        """
        if not text:
            return ""

        # Split string into a list of words
        words = text.split()
        
        # Process list using list comprehension
        corrected_words = [self.normalize_word(word) for word in words]
        
        # Join back into a string
        return " ".join(corrected_words)

    def process_file(self, input_path, output_path):
        print(f"Processing {input_path} -> {output_path}...")
        try:
            with open(input_path, 'r', encoding='utf-8') as infile, \
                 open(output_path, 'w', encoding='utf-8') as outfile:
                
                for line in infile:
                    # Normalize the line
                    normalized_line = self.normalize_sentence(line)
                    # Write to file (add newline as split/join might eat trailing ones)
                    outfile.write(normalized_line + '\n')
            print("Done.")
        except Exception as e:
            print(f"Error processing file: {e}")

# ==========================================
# Usage
# ==========================================

if __name__ == "__main__":
    json_path = 'chillu_pairs_found.json'
    
    normalizer = MalayalamWordNormalizer(json_path)
    normalizer.process_file('sangraha_ZWJ_removed.txt', 'sangraha_subset_chillu_fixed.txt')