#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for complaints-complainants_2000-2016_2016-11'''

import __main__
import pandas as pd
from setup import do_setup
from general_utils import FormatData
from import_functions import read_p046957_file

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_files': [
            'input/p046957_-_report_5.1_-_complainant_(reporting_party)_data.xls',
            'input/p046957_-_report_5.2_-_complainant_(reporting_party)_data.xls',
            'input/p046957_-_report_5.3_-_complainant_(reporting_party)_data.xls'
                       ],
        'output_file': 'output/complaints-complainants_2000-2016_2016-11.csv.gz',
        'column_names': ['cr_id', 'gender', 'age', 'race']

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
                                                  original_crid_col='Number')
    log.info(('Processing {0} file, of FOIA number {1}, produced on {2}'
              '').format(input_file, FOIA_request, report_produced_date))
    df.columns = cons.column_names
    data_df = (data_df
               .append(df)
               .reset_index(drop=True))

FormatData(log=log)\
    .import_data(data_df)\
    .clean()\
    .write_data(cons.output_file)
