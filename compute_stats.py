
import pickle
import datetime
import csv
import re
from textstat.textstat import textstat
from fuzzywuzzy import fuzz
from multiprocessing import Pool


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)

cout('Loading pickle file')

with open('parsed data/table.pkl', 'rb') as f:
    table = pickle.load(f)

numeric = re.compile('.*[0-9].*')

unique = set()

def calc_stat(row):
    # Analyze from and reply_to data
    send_reply_data = list(row.pop('from', {}) | row.pop('reply_to', {}))

    frm_fuzz_set = 0
    n = 0

    if not row['is_spam']:
        pass

    for i in range(len(send_reply_data)-1):
        for j in range(i+1, len(send_reply_data)):
            frm_fuzz_set += fuzz.token_set_ratio(send_reply_data[i], send_reply_data[j])
            n += 1

    row['frm_fuzz_set_ratio'] = frm_fuzz_set / n if n != 0 else 100

    # Analyze payload(s)
    payload = row.pop('payload', None)

    def set_neg1():
        row['payload_smog_index'] = -1
        row['payload_coleman_liau_index'] = -1
        row['payload_dale_chall_readability_score'] = -1

    if payload is not None and payload != '':
        try:
            row['payload_smog_index'] = textstat.smog_index(payload)
            row['payload_coleman_liau_index'] = textstat.coleman_liau_index(payload)
            row['payload_dale_chall_readability_score'] = textstat.dale_chall_readability_score(payload)
        except:
            set_neg1()
    else:
        set_neg1()

    return row

cout('Computing %d row statistics using %d processes' % (len(table), 4))

pool = Pool(processes=4)
table = pool.map(calc_stat, table)

cout('Saving table to disk as CSV')

# Save to disk as CSV
with open('parsed data/table.csv', 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, table[0].keys())
    dict_writer.writeheader()

    for row in table:
        dict_writer.writerow({k: v.encode('utf-8') if isinstance(v, str) else v for k, v in row.items()})
