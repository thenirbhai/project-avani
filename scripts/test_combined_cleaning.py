
import sys
import os

# Add scripts dir to path to import wikipedia_cleaning
sys.path.append(os.path.join(os.path.dirname(__file__)))

from wikipedia_cleaning import _clean_line

test_cases = [
    ("This is a test [p]", "This is a test"),
    ("[c] Start of sentence", "Start of sentence"),
    ("Middle /ai/ of sentence", "Middle of sentence"),
    ("Multiple /t~t/ and /d~d/ markers", "Multiple and markers"),
    ("Complex /i~i~ai~aj~ej~oj/ pattern", "Complex pattern"),
    ("Clean sentence", "Clean sentence"),
    ("[]", None), # Should become empty and return None (due to MIN_LINE_LENGTH check in _clean_line, wait, MIN_LINE_LENGTH is 10)
    ("Short [p]", None), # "Short " -> length 6 < 10 -> None
    ("Long enough sentence with [c] removal included", "Long enough sentence with removal included")
]

print("Running tests for _clean_line...")
failed = 0
for original, expected in test_cases:
    result = _clean_line(original)
    
    # Normalize result (None vs string)
    if result is not None:
        result = result.strip()
        
    if result != expected:
        print(f"FAIL: Input: '{original}'")
        print(f"      Exp:   '{expected}'")
        print(f"      Got:   '{result}'")
        failed += 1
    else:
        print(f"PASS: '{original}' -> '{result}'")

if failed == 0:
    print("\nAll tests passed!")
else:
    print(f"\n{failed} tests failed.")
