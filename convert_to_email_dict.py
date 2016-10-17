
from email.parser import Parser
import glob
import os
import pickle
import datetime
import numpy as np


def cout(text):
    print('[{:%Y-%m-%d %H:%M:%S}] %s'.format(datetime.datetime.now()) % text)

cout('Getting file paths')

# Get file names
ham_paths = [x for x in glob.iglob('raw data/HAM/**', recursive=True) if os.path.isfile(x)]
spam_paths = [x for x in glob.iglob('raw data/SPAM/**', recursive=True) if os.path.isfile(x)]

cout('Calculate file sizes')

file_sizes = (len(ham_paths)+len(spam_paths))*[None]
idx = 0
for path in ham_paths:
    file_sizes[idx] = os.path.getsize(path)
    idx += 1
for path in spam_paths:
    file_sizes[idx] = os.path.getsize(path)
    idx += 1

cout('Find file size mean and standard deviation')

SD_MULTIPLIER = 2.5
file_size_mean = np.mean(file_sizes)
file_size_sd = np.std(file_sizes)
file_size_max = file_size_mean + SD_MULTIPLIER * file_size_sd
tmp = np.max(file_sizes)

cout('Excluding paths whose file size is < %d SD (< %s Bytes)' % (SD_MULTIPLIER, file_size_max))

ham_paths = [path for path in ham_paths if os.path.getsize(path) < file_size_max]
spam_paths = [path for path in spam_paths if os.path.getsize(path) < file_size_max]

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
