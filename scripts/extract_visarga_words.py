import os

def extract_visarga_words(input_file):
    visarga_char = 'ഃ'
    extracted_words = []

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.split('\t')
                if parts:
                    word = parts[0].strip()
                    if visarga_char in word:
                        extracted_words.append(word)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        return []

    return extracted_words

if __name__ == "__main__":
    input_file = "data/datuk.txt"
    # Ensure the path is correct relative to the script location or use absolute path
    # Assuming script is run from project root e:\project-avani
    if not os.path.exists(input_file):
         # Try absolute path if relative fails, or adjust
         input_file = r"e:\project-avani\data\datuk.txt"

    words = extract_visarga_words(input_file)

    if words:
        print(f"Found {len(words)} words with Visarga:")
        for word in words:
            print(word)
    else:
        print("No words with Visarga found.")
