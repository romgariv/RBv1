#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for complaints-witnesses_2000-2016_2016-11'''

import __main__
import numpy as np

from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/complaints-witnesses_2000-2016_2016-11.csv.gz',
        'output_file': 'output/complaints-witnesses_2000-2016_2016-11.csv.gz',
        'intrafile_id': 'complaints-witnesses_2000-2016_2016-11_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'qfilter1' : 'first_name_NS == first_name_NS',
        'qfilter2' : 'appointed_date == appointed_date',
        'profile_cols' : [
            'current_star', 'first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name',
            'middle_initial2', 'race', 'gender', 'birth_year', 'appointed_date'
        ],
        'radd_cols_pre' : [
            {'exec' : ("_DATA_.loc[_DATA_['appointed_date'].isnull(), 'appointed_date']"
                       " = _DATA_.loc[_DATA_['appointed_date'].isnull(), ['start_date', 'org_hire_date']].apply(pd.to_datetime).apply(np.nanmax,axis=1).dt.date")},
        ],
        'rfill_cols' : [
            'first_name_NS', 'last_name_NS', 'appointed_date', 'star',
            'race', 'gender', 'middle_initial', 'middle_initial2',
            'suffix_name', 'birth_year'
        ],
        'radd_cols' : [
            'F4FN', 'F4LN', 'L4LN', 'F3FN', 'L3FN', 'F1FN',
            {'exec' : "_DATA_['FN2']=_DATA_['last_name_NS']"},
            {'exec' : "_DATA_['LN2']=_DATA_['first_name_NS']"}
        ],
        'sadd_cols' : [
            'F4FN', 'F4LN', 'L4LN', 'F3FN', 'L3FN', 'F1FN',
            {'exec' : "_DATA_['FN2']=_DATA_['first_name_NS']"},
            {'exec' : "_DATA_['LN2']=_DATA_['last_name_NS']"},
            {'exec' : "_DATA_['star']=_DATA_['current_star']"}

        ],
        'loop_merge' : {
            'custom_merges' : [
                ['FN2', 'LN2', 'birth_year', 'appointed_date', 'star'],
                ['first_name_NS', 'L4LN', 'birth_year', 'appointed_date'],
                ['F3FN', 'L3FN', 'last_name_NS', 'birth_year', 'appointed_date', 'star'],
                ['first_name_NS', 'star', 'birth_year', 'appointed_date'],
                ['F4FN', 'star', 'birth_year', 'appointed_date'],
                ['F3FN', 'last_name_NS', 'birth_year', 'appointed_date'],
                ['F1FN', 'L3FN', 'birth_year', 'appointed_date', 'star'],
                ['last_name_NS', 'birth_year', 'appointed_date', 'star'],
                ['first_name_NS', 'birth_year', 'appointed_date'],
                ['F1FN','F4LN', 'birth_year', 'appointed_date', 'gender'],
                ['F4LN', 'birth_year', 'appointed_date', 'gender'],
                ['first_name_NS', 'last_name_NS', 'birth_year'],
                ],
           'cycle' : 'sup',
           'mode' : 'otm',
        },
        'drop_cols' : ['F4FN', 'F4LN', 'L4LN', 'F3FN',
                       'L3FN', 'F1FN', 'FN2', 'LN2'],
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

stored_ref = \
FormatData(log=log)\
    .import_data(cons.input_reference_file, uid=cons.uid, add_row_id=False)\
    .data

rfd = \
FormatData(log=log)\
    .import_data(cons.input_reference_file, uid=cons.uid, add_row_id=False)\
    .add_columns(cons.radd_cols_pre)\
    .add_columns(cons.radd_cols)

sfd = \
FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.intrafile_id)\
    .qfilter(cons.qfilter1)\
    .qfilter(cons.qfilter2)\
    .unique([cons.intrafile_id] + cons.profile_cols)\
    .add_columns(cons.sadd_cols)

MergeData(rfd, sfd, log=log)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference(
        inplace=True, drop_cols=cons.drop_cols,
        full_rdf=stored_ref, keep_um=False)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
