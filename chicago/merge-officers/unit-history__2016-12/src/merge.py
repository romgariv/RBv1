#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for unit-history__2016-12'''
import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/unit-history__2016-12.csv.gz',
        'output_file': 'output/unit-history__2016-12.csv.gz',
        'intrafile_id': 'unit-history__2016-12_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'agg_args' : {
            'id_cols': [
                "first_name", "last_name", "suffix_name",
                "first_name_NS", "last_name_NS",
                "appointed_date", "birth_year", "gender"
                ],
            'max_cols': ['middle_initial', 'middle_initial2', 'race'],
            'current_cols': ['unit'],
            'time_col': 'unit_start_date',
            },
        'add_cols' : ['F4FN', 'F4LN', 'L4FN'],
        'drop_cols' : ['F4FN', 'F4LN', 'L4FN'],
        'loop_merge' : {
            'custom_merges' : [
             ['L4FN', 'birth_year', 'race', 'gender', 'current_unit', 'appointed_date'],
             ['race', 'gender', 'birth_year', 'appointed_date', 'last_name_NS'],
             ['first_name_NS', 'last_name_NS', 'birth_year', 'gender'],
             ['first_name_NS', 'last_name_NS', 'gender', 'race', 'current_unit']
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

rfd = \
FormatData(log=log)\
    .import_data(cons.input_reference_file, uid=cons.uid, add_row_id=False)\
    .add_columns(cons.add_cols)

sfd = \
FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.intrafile_id)\
    .aggregate(cons.agg_args)\
    .add_columns(cons.add_cols)

MergeData(rfd, sfd, log=log)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference(inplace=True, drop_cols=cons.drop_cols)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
