#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for complaints-investigators_2000-2016_2016-11'''

import __main__
import pandas as pd
from setup import do_setup
from general_utils import FormatData
from import_functions import read_p046957_file

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_files': [
            'input/p046957_-_report_1.1_-_all_complaints_in_time_frame.xls',
            'input/p046957_-_report_1.2_-_all_complaints_in_time_frame.xls',
            'input/p046957_-_report_1.3_-_all_complaints_in_time_frame.xls',
            'input/p046957_-_report_1.4_-_all_complaints_in_time_frame.xls',
            'input/p046957_-_report_1.5_-_all_complaints_in_time_frame.xls',
            'input/p046957_-_report_1.6_-_all_complaints_in_time_frame.xls'
            ],
        'output_file': 'output/complaints-investigators_2000-2016_2016-11.csv.gz',
        'column_names': [
            'cr_id', 'full_name', 'current_unit',
            'current_rank', 'current_star', 'appointed_date'
            ],
        'ID': 'complaints-investigators_2000-2016_2016-11_ID',
        'auid_args' : {
            'id_cols': [
                'first_name', 'last_name', 'appointed_date',
                'first_name_NS', 'last_name_NS', 'middle_initial'
                ]
            }
    }

    assert all(input_file.startswith('input/')
               for input_file in args['input_files']),\
        "An input_file is malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

data_df = pd.DataFrame()

for input_file in cons.input_files:
    df, report_produced_date, FOIA_request = \
                            read_p046957_file(input_file,
                                              original_crid_col='Number:',
                                              isnull='Number:',
                                              notnull='Location Code:',
                                              drop_col='Beat:')
    log.info(('Processing {0} file, of FOIA number {1}, produced on {2}'
              '').format(input_file, FOIA_request, report_produced_date))
    log.info(('Processing {0} file, of FOIA number {1}, produced on {2}'
              '').format(input_file, FOIA_request, report_produced_date))
    df.columns = cons.column_names
    data_df = (data_df
               .append(df)
               .reset_index(drop=True))

FormatData(log=log)\
    .import_data(data_df)\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .write_data(cons.output_file)
