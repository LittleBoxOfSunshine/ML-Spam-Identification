#!/bin/bash

echo "This script will build the python-compatible data set by running pre-processing scripts and statistic scripts"

mkdir parsed\ data
rm parsed\ data/*                   # Remove any existing files

python3 convert_to_email_dict.py    # Parse raw emails into email object, dump binary to disk
python3 build_table.py              # Filters out unneeded headers, convert to table (also dumps header frequencies)
                                    # Calculate Feature 1
                                    # Calculate Feature 2
                                    # Calculate Feature 3
                                    # Calculate Feature etc
