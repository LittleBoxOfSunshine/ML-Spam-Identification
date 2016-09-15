
import pickle
import datetime
import csv
import re
import copy


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)

cout('Loading pickle file')

with open('parsed data/table.pkl', 'rb') as f:
    table = pickle.load(f)

numeric = re.compile('.*[0-9].*')

unique = set()

for row in table:
    # Analyze from and reply_to data
    frm = row.pop('from', None)
    reply_to = row.pop('reply_to', None)

    # Analyze importance data
    importance = row.pop('importance', None)

    # Filter round
    importance = set(filter(lambda x: not numeric.match(x), importance))

    test = re.compile('list|bulk')

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

    # Analyze subject
    subject = row.pop('subject', None)

    # Analyze payload(s)
    payload = row.pop('payload_raw', None)

    if importance is not None:
        unique.add(importance.pop().lower())

cout('Saving table to disk as CSV')

print(unique)

# # Save to disk as CSV
# with open('parsed data/table.csv', 'w') as output_file:
#     tmp = table[0].keys()
#     dict_writer = csv.DictWriter(output_file, table[0].keys())
#     dict_writer.writeheader()
#
#     for row in table:
#         dict_writer.writerow({k: v.encode('utf-8') if isinstance(v, str) else v for k, v in row.items()})