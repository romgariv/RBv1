#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for complaints-investigators_2000-2018_2018-07'''

import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/complaints-investigators_2000-2018_2018-07.csv.gz',
        'intrafile_id': 'complaints-investigators_2000-2018_2018-07_ID',
        'output_file' : 'output/complaints-investigators_2000-2018_2018-07.csv.gz',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'qfilter' : 'current_unit != 113',
        'profile_cols' : [
                'star', 'first_name', 'last_name', 'first_name_NS', 'last_name_NS', 'current_unit', 'gender',
                'birth_year', 'race', 'middle_initial', 'suffix_name', 'middle_initial2', 'appointed_date'
            ],
        'radd_cols' : ['F4FN', 'F4LN'],
        'sadd_cols' : ['F4FN', 'F4LN'],
        'drop_cols' : ['F4FN', 'F4LN', 'unit'],
        'loop_merge' : {
            'base_OD_edits' : [('birth_year', ['birth_year'])],
            'custom_merges' :[
                ['first_name_NS', 'last_name_NS', 'middle_initial', 'birth_year', 'star'],
                ['first_name_NS', 'last_name_NS', 'current_unit', 'star'],
                ['first_name_NS', 'last_name_NS', 'star']]
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
    .append_to_reference(inplace=True, drop_cols=cons.drop_cols, keep_um=False)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
