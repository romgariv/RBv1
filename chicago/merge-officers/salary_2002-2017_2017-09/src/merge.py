#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for salary_2002-2017_2017-09'''

import __main__
import pandas as pd

from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/salary_2002-2017_2017-09.csv.gz',
        'input_profiles_file' : 'input/salary_2002-2017_2017-09_profiles.csv.gz',
        'output_file': 'output/salary_2002-2017_2017-09.csv.gz',
        'intrafile_id': 'salary_2002-2017_2017-09_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'loop_merge' : {
        'custom_merges' : [
            ['first_name_NS', 'L4LN', 'so_max_date'],
            ['first_name_NS', 'L4LN', 'so_min_date'],
            ['F1FN', 'last_name_NS', 'so_max_date'],
            ['F1FN', 'last_name_NS', 'so_min_date'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'current_age2_mp2'],
            ['first_name_NS', 'last_name_NS', 'current_age_mp'],
            ['first_name_NS', 'last_name_NS', 'current_age_pm'],
            ['first_name_NS', 'last_name_NS', 'current_age_mp'],
            ['first_name_NS', 'last_name_NS', 'current_age2_m1'],
            ['first_name_NS', 'last_name_NS', 'current_age2_p1'],
            ['first_name_NS', 'last_name_NS', 'current_age2_pm'],
            ['first_name_NS', 'last_name_NS', 'current_age2_mp'],
            ['F4FN', 'last_name_NS', 'current_age_pm'],
            ['F4FN', 'last_name_NS', 'current_age_mp'],
            ['F4FN', 'last_name_NS', 'current_age2_pm'],
            ['F4FN', 'last_name_NS', 'current_age2_mp'],
            ['F4FN', 'last_name_NS', 'current_age2_m1'],
            ['F4FN', 'last_name_NS', 'current_age2_p1'],
            {'query': 'gender == "FEMALE"',
             'cols' :['first_name_NS', 'so_max_date', 'current_age_pm', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols' :['first_name_NS', 'so_max_date', 'current_age2_pm', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age_mp', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age2_mp', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age_p1', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age_m1', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age2_m1', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'current_age_pm', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'current_age2_pm', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'current_age_mp', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'current_age2_mp', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'current_age_p1', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'current_age2_p1', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'current_age_m1', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'current_age2_m1', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_min_date', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'middle_initial']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age_mp']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age2_mp']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age_mp']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age2_mp']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age_p1']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age2_p1']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age_m1']},
            {'query': 'gender == "FEMALE"',
             'cols':['first_name_NS', 'so_max_date', 'current_age2_m1']},
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'so_max_year_m1'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'so_min_year_m1'],
            ['first_name_NS', 'last_name_NS', 'current_age_m1', 'resignation_date2'],
            ['middle_initial', 'last_name_NS', 'current_age_mp', 'so_max_date'],
            ],
        'base_OD_edits' :
        [
            ('birth_year', ['birth_year', 'current_age',
                            'current_age_m1', 'current_age2_m1',
                            'current_age_p1', 'current_age2_p1',
                            'current_age_mp', 'current_age2_mp'
                            'current_age_pm','current_age2_pm', '']),
            ('appointed_date', ['so_min_date', 'so_max_date',
                                'so_min_year', 'so_max_year',
                                'so_min_year_m1', 'so_max_year_m1',
                                'resignation_year'])
        ],
        'base_OD' : [
             ('first_name', ['first_name_NS', 'F4FN', 'L5FN']),
             ('last_name', ['last_name_NS', 'F3LN']),
             ('middle_initial', ['middle_initial' , '']),
             ('middle_initial2', ['middle_initial2', '']),
             ('suffix_name', ['suffix_name', '']),
             ('appointed_date', ['appointed_date']),
             ('birth_year', ['birth_year', 'current_age', '']),
             ('gender', ['gender', '']),
             ('race', ['race','']),
             ('current_unit', ['current_unit',''])],

        },
        'radd_cols' : [
            'F4FN', 'F4LN','L4LN', 'F1FN','F3LN', 'L5FN',
             {'exec' : '_DATA_["current_age_p1"], _DATA_["current_age_m1"] = 2017 - _DATA_["birth_year"], 2016 - _DATA_["birth_year"]'},
             {'exec' : "_DATA_['current_age_mp'], _DATA_['current_age_pm'], _DATA_['current_age2_mp'], _DATA_['current_age2_pm'], _DATA_['current_age2_p1'], _DATA_['current_age2_m1'], _DATA_['current_age2_mp2'] = _DATA_['current_age_p1'], _DATA_['current_age_m1'], _DATA_['current_age_p1'], _DATA_['current_age_m1'], _DATA_['current_age_p1'], _DATA_['current_age_m1'], _DATA_['current_age_p1'] + 1"},
             {'exec' : "_DATA_['so_max_date'], _DATA_['so_min_date'] = zip(*_DATA_['appointed_date'].apply(lambda x: (x,x)))"},
             {'exec' : "_DATA_['so_max_year'],_DATA_['so_min_year'], _DATA_['so_min_year_m1'], _DATA_['so_max_year_m1'] = zip(*pd.to_datetime(_DATA_['appointed_date']).dt.year.apply(lambda x: (x,x,x,x)))"},
             {'exec' : "_DATA_['resignation_year'] = pd.to_datetime(_DATA_['resignation_date']).dt.year"},
             {'exec' : "_DATA_['resignation_date2'] = _DATA_['resignation_date']"}],
        'sadd_cols' : [
            'F4FN', 'F4LN','L4LN', 'F1FN','F3LN', 'L5FN',
            {'exec' : "_DATA_['resignation_date2'] = _DATA_['org_hire_date']"},
            {'exec' : "_DATA_['current_age2_mp2'] = _DATA_['current_age_mp']"}],
        'drop_cols' : [
            'F4FN', 'F4LN', 'L4LN', 'F1FN', 'F3LN', 'L5FN',
            'ROWID', 'current_age2_mp2',
            'current_age_m1', 'current_age_p1', 'current_age2_m1',
            'current_age2_p1', 'current_age_mp', 'current_age2_mp',
            'current_age_pm', 'current_age2_pm', 'max_spp_date',
            'so_min_date', 'so_max_date', 'so_min_year', 'so_max_year',
            'so_min_year_m1', 'so_max_year_m1', 'resignation_year',
            'resignation_date2'
            ],

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
    .import_data(cons.input_profiles_file, uid=cons.intrafile_id)\
    .add_columns(cons.sadd_cols)

MergeData(rfd, sfd, log=log)\
    .loop_merge(**cons.loop_merge) \
     .append_to_reference(inplace=True, drop_cols=cons.drop_cols)\
     .remerge_to_file(cons.input_file, cons.output_file)\
     .to_FormatData\
     .write_data(cons.output_reference_file)
