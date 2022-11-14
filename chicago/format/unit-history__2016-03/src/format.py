#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for unit-history__2016-03'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/Kalven_16-1105_All_Sworn_Employees.xlsx',
        'output_file': 'output/unit-history__2016-03.csv.gz',
        'column_names_key': 'unit-history__2016-03',
        'ID': 'unit-history__2016-03_ID',
        'auid_args' : {
            'id_cols': [
                "first_name", "last_name", "middle_initial",
                'middle_initial2', "suffix_name", 'current_age',
                "appointed_date", "gender",
                'first_name_NS', 'last_name_NS'
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
FormatData(log=log)\
    .import_data(cons.input_file, column_names=cons.column_names_key)\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .write_data(cons.output_file)
