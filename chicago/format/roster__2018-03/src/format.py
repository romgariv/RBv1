#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for roster__2018-03'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/P441436-current_and_former_CPD_employee_list_run_15_Mar_2018_by_CPD_IT-redacted_1.xlsx',
        'output_file': 'output/roster__2018-03.csv.gz',
        'column_names_key': 'roster__2018-03',
        'sheet_name' : 'Export Worksheet',
        'ID' : 'roster__2018-03_ID',
        'auid_args' : {
            'id_cols': [
                'first_name', 'first_name_NS', 'last_name', 'last_name_NS',
                'middle_initial', 'middle_initial2', 'suffix_name',
                'appointed_date', 'gender', 'race', 'current_age',
                'current_unit', 'resignation_date', 'current_star'
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
FormatData(log=log)\
    .import_data(cons.input_file, sheets=cons.sheet_name,
                 column_names=cons.column_names_key)\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .write_data(cons.output_file)
