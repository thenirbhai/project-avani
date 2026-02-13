import re

def normalize_malayalam_v3(input_path, output_path):
    # Matches if the NEXT character is NOT a Malayalam character (punctuation, space, etc.)
    malayalam_end = r'(?![\u0020-\u007E\u0D00-\u0D7F])' 

    end_rules = {
        r'ങള്' + malayalam_end: 'ങൾ',
        r'കള്' + malayalam_end: 'കൾ',
        r'ടാന്' + malayalam_end: 'ടാൻ',
        r'കാന്' + malayalam_end: 'കാൻ', 
        r'കാര്' + malayalam_end: 'കാർ',
        r'ല്' + malayalam_end: 'ൽ',
        r'ില്' + malayalam_end: 'ിൽ',  
    }

    anywhere_rules = {
        r'ങള്ക' + malayalam_end: 'ങൾക',
        r'കാര്ക' + malayalam_end: 'കാർക',
        r'ര്മ' + malayalam_end: 'ർമ',
        r'ര്ക' + malayalam_end: 'ർക',
        r'ള്ക' + malayalam_end: 'ൾക'
    }

    try:
        with open(input_path, 'r', encoding='utf-8') as infile:
            content = infile.read()

        # Step 1: Specific clusters anywhere (Specific to General)
        for pattern, replacement in anywhere_rules.items():
            content = re.sub(pattern, replacement, content)

        # Step 2: Word-ends (Context-aware)
        for pattern, replacement in end_rules.items():
            content = re.sub(pattern, replacement, content)

        with open(output_path, 'w', encoding='utf-8') as outfile:
            outfile.write(content)
            
        print("Success: Normalization complete.")

    except Exception as e:
        print(f"Error: {e}")

normalize_malayalam_v3('input.txt', 'output.txt')