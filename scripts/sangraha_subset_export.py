input_file = '../data/sv_mal_cleaned.txt'
output_file = '../data/sv_mal_cleaned_v1_subset.txt'
num_lines = 1000

with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
    for i, line in enumerate(f_in):
        if i >= num_lines:
            break
        f_out.write(line)
print("Done!")