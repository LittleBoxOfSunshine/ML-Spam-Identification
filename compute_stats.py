
import pickle
import datetime
import csv
import re
import copy
import collections
from functools import reduce
from textstat.textstat import textstat
from fuzzywuzzy import fuzz
from scipy.stats import entropy
import multiprocessing as mp
import numpy as np


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)

cout('Loading pickle file')

with open('parsed data/table.pkl', 'rb') as f:
    table = pickle.load(f)

numeric = re.compile('.*[0-9].*')

unique = set()

cout('Loading english word frequency table')

sym_re = re.compile(r'[^a-zA-z]')

freq_table = {}
word_count = 0
total_occurrences = 0
with open('lib data/count_1w.txt') as freq:
    for line in freq:
        name, count = line.partition('\t')[::2]
        count = float(count[:-1])
        freq_table[name.strip()] = count
        word_count += 1
        total_occurrences += count

eng_prob_dist = {}
for key in freq_table:
    eng_prob_dist[key] = freq_table[key] / total_occurrences
eng_prob_dist_list = list(eng_prob_dist.values())
eng_prob_dist_keys = set(eng_prob_dist.keys())
blank_prob_dist = {}
for key in freq_table:
    blank_prob_dist[key] = 0


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

    if payload is not None and payload != '':
        try:
            row['payload_smog_index'] = textstat.smog_index(payload)
            row['payload_coleman_liau_index'] = textstat.coleman_liau_index(payload)
            row['payload_dale_chall_readability_score'] = textstat.dale_chall_readability_score(payload)
            word_freq = collections.Counter(sym_re.sub(' ', payload).lower().split())
            word_freq_keys = [val for val in word_freq if val in eng_prob_dist_keys]
            total = reduce(lambda x, y: x + y, word_freq.values())
            for word in word_freq_keys:
                payload_word_dist[word] = word_freq[word] / total
            row['kl_divergence_eng_lang'] = entropy(pk=list(payload_word_dist.values()), qk=eng_prob_dist_list, base=2)
            row['kl_divergence_eng_lang_e'] = entropy(pk=list(payload_word_dist.values()), qk=eng_prob_dist_list)
        except Exception as e:
            # print(e)
            return None
    else:
        # print('Payload is None or \'\'')
        return None

    return row

cout('Computing %d row statistics using %d processes' % (len(table), 4))

payload_word_dist = copy.deepcopy(blank_prob_dist)

pool = mp.Pool(processes=mp.cpu_count())

itr_count = int(len(table)/1000)
results = [None]*itr_count

for i in range(itr_count):
    results[i] = pool.map(calc_stat, table[i * 1000:i * 1000 + 999])
    cout('%d emails completed' % ((i+1) * 1000))
results[itr_count-1] = pool.map(calc_stat, table[itr_count*1000:])
cout('%d emails completed' % len(table))

cout('Saving table to disk as CSV')

# Save to disk as CSV
with open('parsed data/table2.csv', 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, table[0].keys())
    dict_writer.writeheader()

    for t in results:
        for row in t:
            if row is not None:
                dict_writer.writerow({k: v.encode('utf-8') if isinstance(v, str) else v for k, v in row.items()})
