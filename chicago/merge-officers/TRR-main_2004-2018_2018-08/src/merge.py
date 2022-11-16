#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for TRR-main_2004-2018_2018-08'''

import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/TRR-main_2004-2018_2018-08.csv.gz',
        'output_file': 'output/TRR-main_2004-2018_2018-08.csv.gz',
        'intrafile_id': 'TRR-officers_2004-2018_2018-08_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'qfilter' : 'rank != ["DETENTION AIDE", "9122"]',
        'profile_cols' : [
            'first_name', 'last_name', 'first_name_NS', 'last_name_NS',
            'middle_initial', 'middle_initial2', 'suffix_name',
            'appointed_date', 'gender', 'race', 'current_star'
            ],
        'radd_cols' : ['F4FN', 'F4LN'],
        'sadd_cols' : ['F4FN', 'F4LN', {'exec' : '_DATA_["star"]=_DATA_["current_star"]'}],
        'drop_cols' : ['F4FN', 'F4LN'],
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
    .loop_merge()\
    .append_to_reference(inplace=True, drop_cols=cons.drop_cols)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
