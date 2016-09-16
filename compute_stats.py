
import pickle
import datetime
import csv
import re
from textstat.textstat import textstat
from fuzzywuzzy import fuzz


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)

cout('Loading pickle file')

with open('parsed data/table.pkl', 'rb') as f:
    table = pickle.load(f)

numeric = re.compile('.*[0-9].*')

unique = set()

for row in table[30000:30100]:
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
        importance = {'none'}
    elif all('$' in x or 'user' in x or 'auto' in x for x in importance):
        importance = {'invalid'}
    else:
        importance = {importance.pop().lower()}

    row['importance'] = ', '.join(importance)

    # Analyze subject
    subject = row.pop('subject', None)

    if subject is not None and subject != '':
        row['subject_flesch_reading_ease'] = textstat.flesch_reading_ease(subject)
        row['subject_smog_index'] = textstat.smog_index(subject)
        row['subject_flesch_kincaid_grade'] = textstat.flesch_kincaid_grade(subject)
        row['subject_coleman_liau_index'] = textstat.coleman_liau_index(subject)
        row['subject_automated_readability_index'] = textstat.automated_readability_index(subject)
        row['subject_dale_chall_readability_score'] = textstat.dale_chall_readability_score(subject)
        row['subject_difficult_words'] = textstat.difficult_words(subject)
        row['subject_linsear_write_formula'] = textstat.linsear_write_formula(subject)
        row['subject_gunning_fog'] = textstat.gunning_fog(subject)
        #row['subject_text_standard'] = textstat.text_standard(subject)
    else:
        row['subject_flesch_reading_ease'] = -1
        row['subject_smog_index'] = -1
        row['subject_flesch_kincaid_grade'] = -1
        row['subject_coleman_liau_index'] = -1
        row['subject_automated_readability_index'] = -1
        row['subject_dale_chall_readability_score'] = -1
        row['subject_difficult_words'] = -1
        row['subject_linsear_write_formula'] = -1
        row['subject_gunning_fog'] = -1
        #row['subject_text_standard'] = -1

    # Analyze payload(s)
    payload = row.pop('payload', None)

    if payload is not None and payload != '':
        row['payload_flesch_reading_ease'] = textstat.flesch_reading_ease(payload)
        row['payload_smog_index'] = textstat.smog_index(payload)
        row['payload_flesch_kincaid_grade'] = textstat.flesch_kincaid_grade(payload)
        row['payload_coleman_liau_index'] = textstat.coleman_liau_index(payload)
        row['payload_automated_readability_index'] = textstat.automated_readability_index(payload)
        row['payload_dale_chall_readability_score'] = textstat.dale_chall_readability_score(payload)
        row['payload_difficult_words'] = textstat.difficult_words(payload)
        row['payload_linsear_write_formula'] = textstat.linsear_write_formula(payload)
        row['payload_gunning_fog'] = textstat.gunning_fog(payload)
        #row['payload_text_standard'] = textstat.text_standard(payloa
    else:
        row['payload_flesch_reading_ease'] = -1
        row['payload_smog_index'] = -1
        row['payload_flesch_kincaid_grade'] = -1
        row['payload_coleman_liau_index'] = -1
        row['payload_automated_readability_index'] = -1
        row['payload_dale_chall_readability_score'] = -1
        row['payload_difficult_words'] = -1
        row['payload_linsear_write_formula'] = -1
        row['payload_gunning_fog'] = -1
        #row['payload_text_standard'] = -1

cout('Saving table to disk as CSV')

# Save to disk as CSV
with open('parsed data/table.csv', 'w') as output_file:
    dict_writer = csv.DictWriter(output_file, table[30000].keys())
    dict_writer.writeheader()

    for row in table[30000:30100]:
        dict_writer.writerow({k: v.encode('utf-8') if isinstance(v, str) else v for k, v in row.items()})
