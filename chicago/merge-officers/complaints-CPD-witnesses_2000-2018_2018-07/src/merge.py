#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for complaints-CPD-witnesses_2000-2018_2018-07'''

import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz',
        'output_file': 'output/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz',
        'intrafile_id': 'complaints-CPD-witnesses_2000-2018_2018-07_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'qfilter' : 'likely_sworn_officer==1',
        'profile_cols' : [
                'first_name', 'last_name', 'first_name_NS', 'last_name_NS', 'current_unit', 'gender',
                'birth_year', 'race', 'middle_initial', 'suffix_name', 'middle_initial2', 'rank',
                'employee_id'
            ],
        'radd_cols' : [
            'F4FN', 'F4LN',
            {'exec' : '_DATA_["current_rank_temp"]=_DATA_["current_rank"]'},
            {'exec' : '_DATA_["current_age1"]=_DATA_["current_age"]'},
            {'exec' : '_DATA_["current_age2"]=_DATA_["current_age"]'},
            {'exec' : '_DATA_["current_age3"]=_DATA_["current_age"]'}],
        'sadd_cols' : [
            'F4FN', 'F4LN',
            {'exec' : '_DATA_["current_rank_temp"]=_DATA_["rank"]'},
            {'exec' : '_DATA_["current_age1"]=2018 - _DATA_["birth_year"]'},
            {'exec' : '_DATA_["current_age2"]=2017 - _DATA_["birth_year"]'},
            {'exec' : '_DATA_["current_age3"]=2016 - _DATA_["birth_year"]'}],
        'drop_cols' : ['F4FN', 'F4LN', 'current_rank_temp', 'current_age1', 'current_age2', 'current_age3'],
        'loop_merge' : {
            'base_OD' : [
                ('birth_year', ['birth_year', 'current_age2', 'current_age3', 'current_age1']),
                ('first_name', ['first_name_NS', 'F4FN']),
                ('last_name', ['last_name_NS', 'F4LN']),
                ('middle_initial', ['middle_initial', '']),
                ('middle_initial2', ['middle_initial2', '']),
                ('gender', ['gender', '']),
                ('race', ['race', '']),
                ('suffix_name', ['suffix_name', '']),
                ('current_rank', ['current_rank_temp', 'rank', '']),
                ('current_unit', ['current_unit', '']),
            ],
            'custom_merges' : [
                {'query' : 'gender == "FEMALE"',
                 'cols' : ['first_name_NS', 'birth_year', 'middle_initial', 'race']},
                {'query' : 'gender == "FEMALE"',
                 'cols' : ['first_name_NS', 'birth_year', 'race']}
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
    .add_columns(cons.radd_cols)

sfd = \
FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.intrafile_id)\
    .qfilter(cons.qfilter)\
    .unique([cons.intrafile_id] + cons.profile_cols)\
    .add_columns(cons.sadd_cols)

MergeData(rfd, sfd, log=log)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference(inplace=True, keep_um=False, drop_cols=cons.drop_cols)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
