#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for TRR-main_2004-2018_2018-08'''
import __main__
from setup import do_setup
from general_utils import FormatData
import numpy as np
import pandas as pd
import re

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/P456008 13094-FOIA-P456008-TRRdata Responsive Record Produced By R&A.xlsx',
        'output_file' : 'output/TRR-main_2004-2018_2018-08.csv.gz',
        'sheets' : ['Sheet1', 'Star #'],
        'column_names_key': 'TRR-main_2004-2018_2018-08',
        'ID' : 'TRR-officers_2004-2018_2018-08_ID',
        'auid_args' : {
            'id_cols': [
                'first_name', 'last_name', 'first_name_NS', 'last_name_NS',
                'middle_initial', 'middle_initial2', 'suffix_name',
                'appointed_date', 'gender', 'race', 'current_star'
                ]
        }
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)

cons, log = get_setup()

trr = FormatData(log=log)\
    .import_data(cons.input_file, column_names=cons.column_names_key,
                 sheets=cons.sheets)\
    .dcolumn(['ln_drop', 'fn_drop'])\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .fillna('cb_no', 999)\
    .map('cb_no', lambda x: str(x).replace('-', ''))\
    .map('cb_no', lambda x: x if not str(x).startswith('0.') else str(x).replace('0.', ''))\
    .map('cb_no', lambda x: int(float(x)))\
    .map('cb_no', lambda x: np.nan if x in [999, 99999999, 0, 12345678, 11111111, 1] else x)\
    .map('cb_no', lambda x: float(x))\
    .fillna('event_no', 999)\
    .map('event_no', lambda x: str(x).replace('-', ''))\
    .map('event_no', lambda x: x if not str(x).startswith('0.') else str(x).replace('0.', ''))\
    .map('event_no', lambda x: int(float(x)))\
    .map('event_no', lambda x: np.nan if x in [999, 99999999, 0, 12345678, 11111111, 1] else x)\
    .map('event_no', lambda x: float(x))\
    .map('rd_no', lambda x: x if pd.isnull(x) else re.sub('\W', '', x))\
    .data

# log.info("Creating group union from %s" % (['rd_no', 'cb_no', 'cr_id']))
# trr = union_group(trr, 'prd_no', cols=['rd_no', 'cb_no', 'cr_id'])

FormatData(trr, log=log).write_data(cons.output_file)
