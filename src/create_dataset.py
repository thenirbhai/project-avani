import re
import unicodedata
from datasets import load_dataset, interleave_datasets
from tqdm import tqdm

# --- CONFIGURATION ---
OUTPUT_FILE = "nano_malayalam_corpus.txt"
TARGET_SIZE_LINES = 1_000_000  # 1 Million lines is plenty for a robust tokenizer
BUFFER_SIZE = 10_000           # Shuffle buffer

# Regular Expressions for Cleaning
# 1. Wiki specific: remove '''bold''' and ==Header==
RE_WIKI_CLEAN = re.compile(r"'''|==|\[\[|\]\]")
# 2. General whitespace cleanup
RE_WHITESPACE = re.compile(r"\s+")

def get_malayalam_density(text):
    """
    Returns the percentage of characters that are Malayalam.
    Malayalam Unicode Block: U+0D00 to U+0D7F
    """
    if len(text) == 0: return 0.0
    malayalam_chars = len([c for c in text if '\u0D00' <= c <= '\u0D7F'])
    return malayalam_chars / len(text)

def preprocess_text(text, source_type="general"):
    """
    Central cleaning pipeline.
    """
    if not text: return ""

    # 1. Unicode Normalization (NFC)
    text = unicodedata.normalize('NFC', text)

    # 2. Remove Zero Width spaces (unless ZWNJ - we usually keep ZWNJ for meaning)
    text = text.replace('\u200b', '')

    # 3. Source-specific cleaning
    if source_type == "wiki":
        text = RE_WIKI_CLEAN.sub("", text)

    # 4. Collapse multiple spaces/newlines into single space
    # (For tokenizer training, we usually want one sentence/paragraph per line)
    text = RE_WHITESPACE.sub(" ", text).strip()

    return text

def is_high_quality(text):
    """
    Filter out noise, english menus, and short fragments.
    """
    # Filter 1: Too short? (e.g., "Home", "Page 1")
    if len(text) < 20:
        return False

    # Filter 2: Is it actually Malayalam?
    # We require 40% of the chars to be Malayalam to allow for some English words/numbers.
    if get_malayalam_density(text) < 0.40:
        return False

    return True

def main():
    print("Loading datasets in streaming mode...")

    # 1. Load fw (Malayalam Subset)
    # fw is huge, so we stream it.
    ds_fw = load_dataset("HuggingFaceFW/fineweb-2", name="mal_Mlym", split="train", streaming=True)

    # 2. Load Wikipedia (Malayalam)
    # Wiki is high quality.
    ds_wiki = load_dataset("omarkamali/wikipedia-monthly", "latest.ml", split="train", streaming=True)

    # Select only 'text' column to ensure compatibility
    ds_fw = ds_fw.select_columns(["text"])
    ds_wiki = ds_wiki.select_columns(["text"])

    # 3. Interleave (Mix)
    # We use equal probability (0.5, 0.5) to force the tokenizer to respect formal grammar
    # as much as it respects web text, even though fw is naturally larger.
    mixed_dataset = interleave_datasets(
        [ds_wiki, ds_fw],
        probabilities=[0.5, 0.5],
        seed=42,
        stopping_strategy="first_exhausted"
    )

    print(f"Processing and writing to {OUTPUT_FILE}...")

    line_count = 0
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for i, example in tqdm(enumerate(mixed_dataset)):
            raw_text = example['text']

            # Identify source implicitly (heuristically) or apply general cleaning
            # Since we interleaved, we treat them through the same general pipe
            # but assume standard cleaning works for both.

            cleaned_text = preprocess_text(raw_text, source_type="general")

            if is_high_quality(cleaned_text):
                f.write(cleaned_text + "\n")
                line_count += 1

            if line_count >= TARGET_SIZE_LINES:
                break

    print(f"Done! Created {OUTPUT_FILE} with {line_count} lines.")

if __name__ == "__main__":
    main()