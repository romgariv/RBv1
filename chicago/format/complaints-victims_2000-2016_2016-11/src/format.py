#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for complaints-victims_2000-2016_2016-11'''

import __main__
import pandas as pd
from setup import do_setup
from general_utils import FormatData
from import_functions import read_p046957_file

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/p046957_-_report_4_-_victim_data.xls',
        'output_file': 'output/complaints-victims_2000-2016_2016-11.csv.gz',
        'column_names': ['cr_id', 'gender', 'age', 'race']
        }

    assert args['input_file'].startswith('input/'),\
        "input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

data_df = pd.DataFrame()

data_df, report_produced_date, FOIA_request = \
                        read_p046957_file(cons.input_file,
                                          original_crid_col='Number',
                                          drop_col_val=('Race Desc',
                                                        'end of record'))
log.info(('Processing {0} file, of FOIA number {1}, produced on {2}'
          '').format(cons.input_file, FOIA_request, report_produced_date))
data_df.columns = cons.column_names

FormatData(log=log)\
    .import_data(data_df)\
    .clean()\
    .write_data(cons.output_file)
