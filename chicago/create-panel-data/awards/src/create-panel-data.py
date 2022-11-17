#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''create-panel-data script for awards'''

import __main__
import pandas as pd
import numpy as np

from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/awards_1967-2017_2017-08.csv.gz',
        'output_file' : 'output/monthly-panel_awards_1967-2017.csv.gz',
        'keep_cols' : ['NUID', 'award_type', 'award_request_date', 'current_award_status'],
        'recode_dict' : {
            'HONORABLE MENTION' : 'awd_honment',
            'COMPLIMENTARY LETTER': 'awd_complet',
            'DEPARTMENT COMMENDATION': 'awd_deptcom',
            'ATTENDANCE RECOGNITION AWARD': 'awd_attend',
            'EMBLEM OF RECOGNITION - PHYSICAL FITNESS' : 'awd_physfit'
        }
    }

    assert args['input_file'].startswith('input/'),\
        "Input_files are malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)



cons, log = get_setup()

awd = \
FormatData(log=log)\
    .import_data(cons.input_file, keep_columns=cons.keep_cols, add_row_id=False)\
    .qfilter('NUID == NUID')\
    .date_to_month("award_request_date", "month")\
    .data

awd['award_type'] = awd['award_type']\
    .map(cons.recode_dict)\
    .fillna('awd_other')

awd['current_award_status'] = awd['current_award_status']\
    .replace({'FINAL': 'fin', 'DELETED': 'del', 'DENIED' : 'den'})\
    .fillna('awd_other')

for awd_type in awd['award_type'].unique():
    awd.insert(0, awd_type, (awd['award_type'] == awd_type).astype(int))
    for cas in awd['current_award_status'].unique():
        awd.insert(0, '_'.join([cas, awd_type]),
                   ((awd['award_type'] == awd_type) &
                    (awd['current_award_status'] == cas)).astype(int))

awd = awd.drop(['award_type', 'current_award_status', 'award_request_date'], axis=1)
log.info('Summing by award type by month')
awd = awd\
       .groupby(['NUID', 'month'], as_index=False)\
       .sum()

pd.options.display.max_columns=99
FormatData(awd, log=log).write_data(cons.output_file)
