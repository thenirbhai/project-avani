def count_zero_width_chars(file_path):

    ZWJ = '\u200D'
    ZWNJ = '\u200C'
    ZWNBS = '\ufeff'

    zwj_count = 0
    zwnj_count = 0
    zwnbs_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            zwj_count += line.count(ZWJ)
            zwnj_count += line.count(ZWNJ)
            zwnbs_count += line.count(ZWNBS)
            
    print(f"Results for '{file_path}':")
    print(f"  - Zero Width Joiners (ZWJ): {zwj_count}")
    print(f"  - Zero Width Non-Joiners (ZWNJ): {zwnj_count}")
    print(f"  - Zero Width Non-Breaking Space (ZWNBS): {zwnbs_count}")


file_name = "sangraha_subset.txt" 
count_zero_width_chars(file_name)