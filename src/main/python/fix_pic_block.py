#!/usr/bin/python3
import sys
import time
import urllib.parse

start_time = time.time()

# Input file: query	phot	stock	pic_block	top_position	first_relevant	second_relevant
collect_full_file = "/home/heroys6/Downloads/res_collected_full.csv"
# deposit_file = "/mnt/storage_box/2TB/User/depos_iconv.csv"

# Output format: query  phot stock   pic_block  deposit_top_position    first_relevant  second_relevant
output_file = open("res_pic_block_fix.csv", "w")

# Pic block fix file
new_pics = "/home/heroys6/Downloads/pic_block_fix_Dec-27_13-53-25.txt"

# Global vars
rows_read = 0
new_pics_dict = {}
new_pic_blocks = 0
affected_rows = 0


def print_(message):
    print(message)
    sys.stdout.flush()


print_("Start processing\n")
print_("Start creating pic_block_dict\n")

with open(new_pics, encoding="utf-8") as file:
    try:
        for line in file:
            spl = line.split("\t")

            query = spl[0].lower()
            pic_block = spl[1].strip("\n")

            if pic_block == "1":
                new_pics_dict[query] = pic_block
                new_pic_blocks += 1
    except Exception as e:
        output_file.close()
        print_("")
        print_("[Exception] during creating new_pics_dict")
        print_("[Exception] Rows read: " + str(rows_read))
        print_("[Exception] Exception message: " + str(e))
        print_("[Exception] Time: " + str(time.time() - start_time) + " sec")

print_("Done creating pic_block_dict, time: " + str(time.time() - start_time) +
       " sec, new pic_blocks: " + str(new_pic_blocks) + "\n")

# Write header
output_file.write("query\tphot\tstock\tpic_block\ttop_position\tfirst_relevant\tsecond_relevant\n")

with open(collect_full_file, encoding="utf-8") as file:
    try:
        for line in file:  # line format: query    link    position  pic_block   anchor  snippet\n
            rows_read += 1

            if (rows_read == 1): # skip header
                continue

            splitted = line.split("\t")

            query = splitted[0].lower()
            phot = splitted[1]
            stock = splitted[2]
            pic_block = splitted[3]
            top_p = splitted[4]
            f_rel = splitted[5]
            s_rel = splitted[6].strip("\n")

            # Group by query
            if pic_block == "0":
                if query in new_pics_dict:
                    pic_block = "1"
                    affected_rows += 1

            output_file.write(query + "\t" +
                              phot + "\t" +
                              stock + "\t" +
                              pic_block + "\t" +
                              top_p + "\t" +
                              f_rel + "\t" +
                              s_rel + "\n")

    except Exception as e:
        output_file.close()
        print_("")
        print_("[Exception] Rows read: " + str(rows_read))
        print_("[Exception] Exception message: " + str(e))
        print_("[Exception] Time: " + str(time.time() - start_time) + " sec")
        quit()

output_file.close()
print_("")
print_("[RESULT] Done successfully")
print_("[RESULT] Read rows: " + str(rows_read))
print_("[RESULT] Affected rows: " + str(affected_rows))
print_("[RESULT] Time: " + str(time.time() - start_time) + " sec")
