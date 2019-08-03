#!/usr/bin/python3
import sys
import time
import urllib.parse

start_time = time.time()

# Input file: query    link    position  pic_block   anchor  snippet
# deposit_file = "test.csv"
deposit_file = "/mnt/storage_box/2TB/User/depos_iconv.csv"

# Output format: query  phot stock   pic_block  deposit_top_position    first_relevant  second_relevant
output_file = open("collected_full.csv", "w")

# Global vars
last_q = ""
top100 = []  # format: (q, url, pos, anc, pic_b)
rows_read = 0
all_rows_in_prod_file = 397236345
stock_words = [
    'сток',
    'стоки',
    'стока',
    'стоков',
    'стоку',
    'стокам',
    'стоком',
    'стоками',
    'стоке',
    'стоках',
    'стоковый',
    'стоковые',
    'стокового',
    'стоковых',
    'стоковому',
    'стоковым',
    'стоковыми',
    'стоковом',
    'стоковая',
    'стоковой',
    'стоковую',
    'стоковое',
    'фотосток',
    'фотостоки',
    'фотостока',
    'фотостоков',
    'фотостоку',
    'фотостокам',
    'фотостоком',
    'фотостоками',
    'фотостоке',
    'фотостоках',
    'stock'
]


def is_stock(anc, snipp):
    def is_stock(str):
        for word in str.lower().split(" "):
            w_clr = word.strip(".,:")
            if w_clr in stock_words:
                # print("Here stock was found: " + str)
                return True
        return False

    return is_stock(anc) or is_stock(snipp)


def is_phot(anc, snipp):
    def is_phot(str):
        if 'фото' in str.lower():
            return True
        return False

    return is_phot(anc) or is_phot(snipp)


def print_(message):
    print(message)
    sys.stdout.flush()


def process_done_group():
    global top100
    global output_file

    # top100 element format: (q, url, pos, anc, pic_b, snipp)

    sorted(top100, key=lambda row: row[2])  # sort by position

    query = top100[0][0]
    phot = 0
    stock = 0
    pic_block = top100[0][4]
    deposit_top_position = "-"
    first_relevant = "-"
    second_relevant = "-"

    top10 = top100[:10]
    top15 = top100[:15]

    # Search for phot
    for row in top10:
        if is_phot(row[3], row[5]):
            phot += 1
    phot = (float(phot) / len(top10)) * 100

    # Search for stock
    for row in top15:
        if is_stock(row[3], row[5]):
            stock += 1
    stock = (float(stock) / len(top15)) * 100

    deposit_rows = []

    for row in top100:
        if "ru.depositphotos" in row[1]:
            deposit_rows.append(row)

    if len(deposit_rows) == 0:
        pass  # default values
    elif len(deposit_rows) == 1:
        deposit_top_position = deposit_rows[0][2]
        first_relevant = urllib.parse.unquote(deposit_rows[0][1])
    else:
        deposit_top_position = deposit_rows[0][2]
        first_relevant = urllib.parse.unquote(deposit_rows[0][1])
        second_relevant = urllib.parse.unquote(deposit_rows[1][1])

    # Write out result row: query  phot stock   pic_block  deposit_top_position    first_relevant  second_relevant
    output_file.write(str(query) + "\t"
                      + str(phot) + "\t"
                      + str(stock) + "\t"
                      + str(pic_block) + "\t"
                      + str(deposit_top_position) + "\t"
                      + str(first_relevant) + "\t"
                      + str(second_relevant) + "\n")


print_("Start processing\n")

# Write header
output_file.write("query\tphot\tstock\tpic_block\ttop_position\tfirst_relevant\tsecond_relevant\n")

with open(deposit_file, encoding="utf-8") as file:
    try:
        for line in file:  # line format: query    link    position  pic_block   anchor  snippet\n
            rows_read += 1
            splitted = line.split("\t")
            q = splitted[0]
            url = splitted[1]
            pos = splitted[2]
            pic_b = splitted[3]
            anc = splitted[4]
            snipp = splitted[5].strip("\n")

            # Group by query
            if q == last_q or rows_read == 1:
                top100.append((q, url, pos, anc, pic_b, snipp))
            else:
                process_done_group()
                # Add first elem of new group
                top100.clear()
                top100.append((q, url, pos, anc, pic_b, snipp))

            last_q = q
    except Exception as e:
        output_file.close()
        print_("")
        print_("[Exception] Rows read: " + str(rows_read))
        print_("[Exception] Exception message: " + str(e))
        print_("[Exception] Time: " + str(time.time() - start_time) + " sec")
        quit()

process_done_group()
output_file.close()
print_("")
print_("[RESULT] Done successfully")
print_("[RESULT] Read rows: " + str(rows_read))
print_("[RESULT] Time: " + str(time.time() - start_time) + " sec")
