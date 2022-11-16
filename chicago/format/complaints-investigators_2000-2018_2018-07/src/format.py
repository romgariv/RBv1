#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for complaints-investigators_2000-2018_2018-07'''

import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/investigators_export.xlsx',
        'output_file': 'output/complaints-investigators_2000-2018_2018-07.csv.gz',
        'column_names_key': 'complaints-investigators_2000-2018_2018-07',
        'ID' : 'complaints-investigators_2000-2018_2018-07_ID',
        'auid_args' : {
            'id_cols': [
                'first_name_NS', 'last_name_NS', 'birth_year'
             ],
            'conflict_cols': [
                'middle_initial', 'middle_initial2', 'suffix_name',
                'current_unit'
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
