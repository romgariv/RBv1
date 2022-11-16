#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for unit-history__2016-03'''
import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/unit-history__2016-03.csv.gz',
        'output_file': 'output/unit-history__2016-03.csv.gz',
        'intrafile_id': 'unit-history__2016-03_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'agg_args' : {
            'id_cols': [
                "first_name", "last_name", "middle_initial",
                'middle_initial2', "suffix_name", 'current_age',
                "appointed_date", "gender",
                'first_name_NS', 'last_name_NS'
                ],
            'max_cols': [
                'race',
                'star1', 'star2', 'star3', 'star4', 'star5',
                'star6', 'star7', 'star8', 'star9', 'star10'
                ],
            'current_cols': ['unit'],
            'time_col': 'unit_start_date',
            },
        'radd_cols' : [
            'F4FN', 'F4LN', 'L4FN',
            {'exec' : '_DATA_["current_age_m1"] = _DATA_["current_age"]'},
            {'exec' : '_DATA_["current_age_p1"] = _DATA_["current_age"]'}],
        'sadd_cols' : [
            'F4FN', 'F4LN', 'L4FN',
            {'exec' : '_DATA_["current_age_m1"] = _DATA_["current_age"]'},
            {'exec' : '_DATA_["current_age_p1"] = _DATA_["current_age"] + 1'}],
        'drop_cols' : ['F4FN', 'F4LN', 'L4FN', 'current_age_m1', 'current_age_p1'],
        'loop_merge' : {
            'base_OD_edits' : [
                ('birth_year', ['curret_age_p1', 'current_age_m1', ''])
            ],
            'custom_merges' : [
                {'query' : 'gender=="FEMALE"', 'cols':['first_name_NS', 'star', 'gender', 'race', 'appointed_date']},
                ['star', 'appointed_date', 'current_unit', 'current_age_p1', 'gender'],
                {'query' : 'gender=="FEMALE"', 'cols':['first_name_NS', 'appointed_date', 'current_unit', 'gender', 'race']},
                ['first_name_NS', 'last_name_NS', 'current_age_p1'],
                ['first_name_NS', 'last_name_NS']
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

rfd = \
FormatData(log=log)\
    .import_data(cons.input_reference_file, uid=cons.uid, add_row_id=False)\
    .add_columns(cons.radd_cols)

sfd = \
FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.intrafile_id)\
    .aggregate(cons.agg_args)\
    .reshape_long('star')\
    .add_columns(cons.sadd_cols)

MergeData(rfd, sfd, log=log)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference(inplace=True, drop_cols=cons.drop_cols)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
