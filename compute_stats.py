
import pickle
import datetime
import csv
import re
import copy
from fuzzywuzzy import fuzz


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)

cout('Loading pickle file')

with open('parsed data/table.pkl', 'rb') as f:
    table = pickle.load(f)

numeric = re.compile('.*[0-9].*')

unique = set()

for row in table:
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

    # Analyze importance data
    importance = row.pop('importance', None)

    # Filter round
    importance = set(filter(lambda x: not numeric.match(x), importance))

    # Conversion round
    if any(('list' in x or 'ulk' in x) for x in importance):
        importance = {'list'}
    elif any(('ormal' in x or 'non-urgent' in x) for x in importance):
        importance = {'medium'}
    elif any(('igh' in x or 'rgent' in x) for x in importance):
        importance = {'high'}
    elif any('junk' in x for x in importance):
        importance = {'low'}
    elif len(importance) == 0 or importance == {''}:
        importance = None
    elif all('$' in x or 'user' in x or 'auto' in x for x in importance):
        importance = {'invalid'}
    else:
        importance = {importance.pop().lower()}

    row['importance'] = importance

    # Analyze subject
    subject = row.pop('subject', None)

    # Analyze payload(s)
    payload = row.pop('payload ', None)

cout('Saving table to disk as CSV')

# Save to disk as CSV
with open('parsed data/table.csv', 'w') as output_file:
    tmp = table[0].keys()
    print(tmp)
    dict_writer = csv.DictWriter(output_file, table[0].keys())
    dict_writer.writeheader()

    for row in table:
        dict_writer.writerow({k: v.encode('utf-8') if isinstance(v, str) else v for k, v in row.items()})
