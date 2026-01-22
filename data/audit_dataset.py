import random
import unicodedata
from pathlib import Path

def generate_llm_sample(input_file, output_file, num_samples=100):
    """
    Randomly samples lines from a large file and formats them for LLM analysis.
    """
    # 1. Count total lines (efficiently)
    print("Counting lines...")
    with open(input_file, 'rb') as f:
        line_count = sum(1 for _ in f)

    # 2. Determine which lines to pick
    if line_count <= num_samples:
        sampled_indices = set(range(line_count))
    else:
        sampled_indices = set(random.sample(range(line_count), num_samples))

    print(f"Sampling {len(sampled_indices)} random lines from {line_count} total lines...")

    samples = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i in sampled_indices:
                # Basic cleaning for readability
                clean_line = line.strip()
                if clean_line:
                    samples.append(clean_line)

    # 3. Format the output for Gemini
    header = f"--- DATASET ANALYSIS SAMPLE (Source: {input_file}) ---\n"
    header += f"Total Samples: {len(samples)}\n"
    header += "Instructions: Analyze the following Malayalam text for: \n"
    header += "1. Unicode artifacts (broken chillus, ZWJ issues)\n"
    header += "2. Language purity (English/Malayalam ratio)\n"
    header += "3. Formal vs Informal balance.\n"
    header += "--------------------------------------------------\n\n"

    with open(output_file, 'w', encoding='utf-8') as f_out:
        f_out.write(header)
        for idx, s in enumerate(samples):
            f_out.write(f"SAMPLE #{idx+1}:\n{s}\n\n")

    print(f"Sample report generated: {output_file}")

# Usage
generate_llm_sample("../data/normalized_sangraha_subset.txt", "../data/text_samples.txt", num_samples=150)