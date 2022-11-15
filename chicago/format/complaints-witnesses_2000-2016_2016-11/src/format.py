#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for complaints-witnesses_2000-2016_2016-11'''

import __main__
import pandas as pd
from setup import do_setup
from general_utils import FormatData
from import_functions import read_p046957_file

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/p046957_-_report_3_-_police_officer_witness_data_xi.xls',
        'output_file': 'output/complaints-witnesses_2000-2016_2016-11.csv.gz',
        'column_names': [
                         'cr_id', 'full_name', 'gender', 'race',
                         'current_star', 'birth_year', 'appointed_date'
                        ],
        'ID': 'complaints-witnesses_2000-2016_2016-11_ID',
        'auid_args' : {
            'id_cols': ['first_name_NS', 'last_name_NS'],
            'conflict_cols': [
                'middle_initial', 'middle_initial2', 'suffix_name',
                'appointed_date', 'birth_year', 'current_star'
                ]
            }
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

data_df, report_produced_date, FOIA_request = \
                    read_p046957_file(cons.input_file,
                                      original_crid_col='Gender',
                                      drop_col_val=('Race', 'end of record'),
                                      original_crid_mixed=True,
                                      add_skip=0)
log.info(('Processing {0} file, of FOIA number {1}, produced on {2}'
          '').format(cons.input_file, FOIA_request, report_produced_date))
data_df.columns = cons.column_names

FormatData(log=log)\
    .import_data(data_df)\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .write_data(cons.output_file)
