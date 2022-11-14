#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for roster_1936-2017_2017-04'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/P058155_-_Kiefer.xlsx',
        'output_file': 'output/roster_1936-2017_2017-04.csv.gz',
        'metadata_file': 'output/metadata_roster_1936-2017_2017-04.csv.gz',
        'column_names_key': 'roster_1936-2017_2017-04',
        'ID': 'roster_1936-2017_2017-04_ID',
        'clean_args' : {
            'clean_dict' : {
                'current_status' : {'Y' : 1, 'N' : 0}
            }
        },
        'auid_args' : {
            'id_cols': [
                "first_name", "last_name", "first_name_NS", "last_name_NS",
                'suffix_name', "appointed_date", "gender",
                'birth_year', 'current_age', 'resignation_date', 'current_rank'
                ],
            'conflict_cols': [
                'middle_initial', 'middle_initial2',
                'star1', 'star2', 'star3', 'star4', 'star5',
                'star6', 'star7', 'star8', 'star9', 'star10'
                ],
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
    .dcolumn('star11')\
    .clean(clean_args_dict=cons.clean_args)\
    .assign_unique_ids(cons.ID, cons.auid_args)\
    .write_data(cons.output_file)
