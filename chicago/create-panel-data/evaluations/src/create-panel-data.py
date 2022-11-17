#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''create-master-data script for evaluations'''

import __main__
import pandas as pd

from setup import do_setup
from general_utils import FormatData, keep_duplicates

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/evaluations-officers_2009-2017_2017-04.csv.gz',
        'input_sup_file' : 'input/evaluations-supervisors_2009-2017_2017-04.csv.gz',
        'input_profiles_file' : 'input/officer-profiles.csv.gz',
        'output_file' : 'output/monthly-panel_evaluations_2009-2017.csv.gz',
        'keep_cols' : ['ROWID', 'assigned_date', 'NUID']
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file are malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

po = FormatData(log=log)\
    .import_data(cons.input_file, add_row_id=False)\
    .kcolumn(cons.keep_cols + ['evaluation_year'])\
    .set_columns({'NUID' : 'po_NUID', 'assigned_date' : 'po_assigned_date'})\
    .data

sup = FormatData(log=log)\
    .import_data(cons.input_sup_file, add_row_id=False)\
    .kcolumn(cons.keep_cols)\
    .set_columns({'NUID' : 'sup_NUID', 'assigned_date' : 'sup_assigned_date'})\
    .data

assert po[['ROWID']].equals(sup[['ROWID']])

df = po.merge(sup, on=['ROWID'])
df[['ROWID', 'evaluation_year']].equals(po[['ROWID', 'evaluation_year']])

df = df.dropna(subset=['po_NUID'])
panels = pd.DataFrame({'month' : pd.date_range('2009-01-01', '2017-12-01', freq='MS')})
panels['evaluation_year'] = panels['month'].dt.year

rows = df.shape[0]
df = df.merge(panels, on=['evaluation_year'])

FormatData(df, log=log)\
    .dcolumn('ROWID')\
    .add_columns([
        {'exec' : '_DATA_["redacted_sup"] = _DATA_["sup_NUID"].isnull().astype(int)'}
    ])\
    .write_data(cons.output_file)
