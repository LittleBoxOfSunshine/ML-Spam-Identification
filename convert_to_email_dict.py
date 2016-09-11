
from email.parser import Parser
import glob
import os
import pickle
import datetime


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)

cout('Getting file paths')

# Get file names
ham_paths = [x for x in glob.iglob('raw data/HAM/**', recursive=True) if os.path.isfile(x)]
spam_paths = [x for x in glob.iglob('raw data/SPAM/**', recursive=True) if os.path.isfile(x)]

cout('Allocating Memory')

emails = {'ham': len(ham_paths)*[None], 'spam': len(spam_paths)*[None]}

cout('Load and parse HAM')

# Load ham files
idx = 0
for path in ham_paths:
    with open(path, encoding='latin1') as fp:
        emails['ham'][idx] = Parser().parse(fp)
        idx += 1

cout('Load and parse SPAM')

# Load spam files
idx = 0
for path in spam_paths:
    with open(path, encoding='latin1') as fp:
        emails['spam'][idx] = Parser().parse(fp)
        idx += 1

cout('Saving to disk')

# Save to disk
with open('parsed data/emails.pkl', 'wb') as f:
    pickle.dump(emails, f, pickle.HIGHEST_PROTOCOL)

cout('Process Completed')

# def save_obj(obj, name ):
#     with open('obj/'+ name + '.pkl', 'wb') as f:
#         pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
#
# def load_obj(name ):
#     with open('obj/' + name + '.pkl', 'rb') as f:
#         return pickle.load(f)

#  Now the header items can be accessed as a dictionary:
# print('To: %s' % headers['to'])
# print('From: %s' % headers['from'])
# print('Subject: %s' % headers['subject'])

# if headers.is_multipart():
#     for payload in headers.get_payload():
#         print(payload.get_payload())
# else:
#     print(headers.get_payload())
