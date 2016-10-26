
import pickle
import datetime
import csv
import re
import copy
from bs4 import BeautifulSoup


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)


def post_filter_header_frequency_analysis(data, file_name, keys):
    header_counts = {}
    for h in keys:
        header_counts[h] = {'Header': h, 'SPAM Count': 0, 'HAM Count': 0}

    cout('Counting occurrences of header labels')

    for row in data:
        for h in row:
            if h in keep_headers and row[h] is not None:
                header_counts[h]['SPAM Count' if row['is_spam'] else 'HAM Count'] += 1

    cout('Saving occurrence counts to disk')

    with open('parsed data/%s.csv' % file_name, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, ['Header', 'SPAM Count', 'HAM Count'])
        dict_writer.writeheader()
        dict_writer.writerows(header_counts.values())


def header_frequency_analysis(data, file_name, keys):
    header_counts = {}
    for h in keys:
        header_counts[h] = {'Header': h, 'SPAM Count': 0, 'HAM Count': 0}

    cout('Counting occurrences of header labels from SPAM')

    for datum in data['spam']:
        for d in datum:
            header_counts[d]['SPAM Count'] += 1

    cout('Counting occurrences of header labels from HAM')

    for datum in data['ham']:
        for d in datum:
            header_counts[d]['HAM Count'] += 1

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
header_frequency_analysis(emails, 'pre_filter_headers', headers)

# Contains header-regex key-value pairs that will be kept after filtering
keep_headers = {
    'from': re.compile('from|.*sender'),                                     # Categorical (match, no-match, n/a)
    'reply_to': re.compile('^(?!in).*reply.*to.*$|returnpath'),              # Categorical (match, no-match, n/a)
    'cc': re.compile('cc'),                                                  # Bool for has header
    'organization': re.compile('.*organization.*')                           # Bool for has header
}

cout('Building table template (layout)')

# Initialize template with all None values
row_template = {
    'is_spam': None,
    'from': set(),
    'reply_to': set(),
    'cc': False,
    'organization': False
}

# Allocate table of templates
table = (len(emails['spam']) + len(emails['ham'])) * [None]

cout('Filtering & combining headers / importing HAM into table')


def html2txt(html):
    text = ''

    try:
        soup = BeautifulSoup(html, "lxml")

        # kill all script and style elements
        for script in soup(["script", "style"]):
            script.extract()  # rip it out

        # get text
        text = soup.get_text()

        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
    except:
        print('ERROR: Failed to parse as HTML => %s' % html)

    return text

import base64


def flatten(msg):
    if isinstance(msg, str):
        return msg

    ret = ''

    try:
        for m in msg:
            payload = m.get_payload()
            if isinstance(payload, list):
                #ret += flatten(payload)
                continue

            tmp = m.get('Content-Type')

            if tmp is None or 'text' in tmp:
                if m.get('Content-Transfer-Encoding') == 'base64':
                    html = base64.b64decode(payload)
                ret += str(payload)
            elif 'text/html' in tmp:
                if m.get('Content-Transfer-Encoding') == 'base64':
                    payload = base64.b64decode(payload)
                ret += html2txt(payload)
    except:
        print('ERROR: Failed to parse => %s' % msg)

    return ret

char_filter = re.compile('[^a-z]')

idx = 0

for email in emails['ham']:
    table[idx] = copy.deepcopy(row_template)
    table[idx]['is_spam'] = False

    # Find relevant headers
    for header in email:
        for key in keep_headers:
            # Check lowercase, alphabet only characters against patterns
            tmp = char_filter.sub('', header.lower())
            if keep_headers[key].search(tmp):
                if table[idx][key] is None:
                    table[idx][key] = email[header]
                elif table[idx][key] is False:
                    table[idx][key] = True
                elif table[idx][key] is True:
                    pass
                elif isinstance(table[idx][key], set):
                    table[idx][key].add(email[header])

    table[idx]['multipart_count'] = 0 if not email.is_multipart() else len(email.get_payload())
    table[idx]['payload'] = flatten(email.get_payload())

    idx += 1

cout('Filtering & combining headers / importing SPAM into table')

for email in emails['spam']:
    table[idx] = copy.deepcopy(row_template)
    table[idx]['is_spam'] = True

    # Find relevant headers
    for header in email:
        for key in keep_headers:
            # Check lowercase, alphabet only characters against patterns
            tmp = char_filter.sub('', header.lower())
            if keep_headers[key].search(tmp):
                if table[idx][key] is None:
                    table[idx][key] = email[header]
                elif table[idx][key] is False:
                    table[idx][key] = True
                elif table[idx][key] is True:
                    pass
                elif isinstance(table[idx][key], set):
                    table[idx][key].add(email[header])

    table[idx]['multipart_count'] = 0 if not email.is_multipart() else len(email.get_payload())
    table[idx]['payload'] = flatten(email.get_payload())

    idx += 1

cout('Saving to disk')

# Save to disk
with open('parsed data/table.pkl', 'wb') as f:
    pickle.dump(table, f, pickle.HIGHEST_PROTOCOL)

cout('Starting post-filter header frequency analysis')
post_filter_header_frequency_analysis(table, 'post_filter_headers', keep_headers.keys())

