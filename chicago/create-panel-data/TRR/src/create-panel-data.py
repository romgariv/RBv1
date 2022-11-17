#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''create-master-data script for TRR-actions-responses'''
import __main__
import pandas as pd
import numpy as np

from setup import do_setup
from general_utils import FormatData, keep_duplicates

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/TRR-member-actions_2004-2018_2018-08.csv.gz',
        'input_TO_file' : 'input/TRR-main_2004-2018_2018-08.csv.gz',
        'output_file' : 'output/monthly-panel_TRR_2004-2018.csv.gz'
    }

    assert args['input_file'].startswith('input/'),\
        "Input_files are malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

ar = FormatData(log=log)\
    .import_data(cons.input_file)\
    .data

ar = ar[
    ['trr_id'] + [c for c in ar.columns if c.startswith('poaction_')]
        ].drop_duplicates()

tm = FormatData(log=log)\
    .import_data(cons.input_TO_file)\
    .kcolumn(['trr_id', 'NUID', 'trr_date', 'injured', 'sub_injured'])\
    .date_to_month('trr_date', 'month')\
    .dcolumn('trr_date')\
    .data
trr = tm[['NUID', 'month', 'injured', 'sub_injured', 'trr_id']]
trr.insert(0, 'poaction_missing', 1 - trr.trr_id.isin(ar.trr_id).astype(int))
del trr['trr_id']
trr.insert(0,'TRR', 1)
trr.insert(0, 'TRR_po_injured', (trr['injured'].fillna('no').str.lower() == 'yes').astype(int))
trr.insert(0, 'TRR_sub_injured', (trr['sub_injured'].fillna('no').str.lower() == 'yes').astype(int))

trr = trr[['NUID', 'month', 'TRR', 'TRR_po_injured', 'TRR_sub_injured', 'poaction_missing']]\
    .groupby(['NUID', 'month'], as_index=False)\
    .sum()

ar = ar\
       .merge(tm, on='trr_id', how='inner')\
       .drop('trr_id', axis=1)\
       .groupby(['NUID', 'month'], as_index=False)\
       .sum()

trr = trr\
    .merge(ar, on=['NUID', 'month'], how='outer')\
    .fillna(0)

assert keep_duplicates(trr, ['NUID', 'month']).empty
assert trr[trr.TRR > trr[trr.columns[trr.columns.str.startswith('poaction')]].sum(axis=1)].empty

FormatData(trr, log=log).write_data(cons.output_file)
