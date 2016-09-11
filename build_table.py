
import pickle
import datetime
import csv


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)


def header_frequency_analysis(data, file_name):
    header_counts = {}
    for h in headers:
        header_counts[h] = {'Header': h, 'SPAM Count': 0, 'HAM Count': 0}

    cout('Counting occurrences of header labels from SPAM')

    for datum in data['spam']:
        for header in datum:
            header_counts[header]['SPAM Count'] += 1

    cout('Counting occurrences of header labels from HAM')

    for datum in data['ham']:
        for header in datum:
            header_counts[header]['HAM Count'] += 1

    cout('Saving occurrence counts to disk')

    with open('parsed data/%s.csv' % file_name, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, ['Header', 'SPAM Count', 'HAM Count'])
        dict_writer.writeheader()
        dict_writer.writerows(header_counts.values())

cout('Loading pickle file')

with open('parsed data/emails.pkl', 'rb') as f:
    emails = pickle.load(f)

headers = set()

cout('Extracting unique header labels from HAM')

for email in emails['ham']:
    for key in email:
        headers.add(key)

cout('Extracting unique header labels from SPAM')

for email in emails['spam']:
    for key in email:
        headers.add(key)

cout('Starting pre-filter header frequency analysis')
header_frequency_analysis(emails, 'pre_filter_headers')

cout('Filtering unneeded headers and combining equivalent headers')
# TODO: Filter and combine headers here

cout('Starting post-filter header frequency analysis')
header_frequency_analysis(emails, 'post_filter_headers')

cout('Building table template (layout)')

row_template = {'Spam': None}

for col in headers:
    row_template[col] = None

table = (len(emails['spam']) + len(emails['ham'])) * [row_template]

cout('Importing HAM into table')


def get_max_depth(payload, depth=0):
    max_depth = depth

    if not isinstance(payload, list):
        return max_depth

    for p in payload:
        tmp = get_max_depth(p, depth+1)
        if tmp > max_depth:
            max_depth = tmp

    return max_depth

idx = 0

for email in emails['ham']:
    table[idx]['Spam'] = False

    for key in email:
        table[idx][key] = email[key]

    table[idx]['multipart_payload'] = email.is_multipart()
    table[idx]['multipart_count'] = 0 if not email.is_multipart() else len(email.get_payload())
    table[idx]['multipart_depth'] = 0 if not email.is_multipart() else get_max_depth(email.get_payload)
    table[idx]['payload_raw'] = email.get_payload()

    idx += 1

cout('Importing SPAM into table')

for email in emails['spam']:
    table[idx]['Spam'] = True

    for key in email:
        table[idx][key] = email[key]

    table[idx]['multipart_payload'] = email.is_multipart()
    table[idx]['payload_raw'] = email.get_payload()

    idx += 1

cout('Saving to disk')

# Save to disk
with open('parsed data/table.pkl', 'wb') as f:
    pickle.dump(emails, f, pickle.HIGHEST_PROTOCOL)
