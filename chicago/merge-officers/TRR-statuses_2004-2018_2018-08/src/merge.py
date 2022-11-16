#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for TRR-statuses_2004-2018_2018-08'''

import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/TRR-statuses_2004-2018_2018-08.csv.gz',
        'output_file': 'output/TRR-statuses_2004-2018_2018-08.csv.gz',
        'intrafile_id': 'TRR-statuses_2004-2018_2018-08_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'qfilter' : 'rank == rank',
        'profile_cols' : [
            'first_name', 'last_name', 'suffix_name', 'star',
            'first_name_NS', 'last_name_NS', 'middle_initial',
            'middle_initial2', 'appointed_date', 'gender', 'race',
            ],
        'radd_cols' : ['F4FN',
            {'exec' : '_DATA_["appointed_date2"] = _DATA_["org_hire_date"]'},
            {'exec' : '_DATA_["appointed_date3"] = _DATA_["start_date"]'}],
        'sadd_cols' : ['F4FN',
            {'exec' : '_DATA_["appointed_date2"] = _DATA_["appointed_date"]'},
            {'exec' : '_DATA_["appointed_date3"] = _DATA_["appointed_date"]'}],
        'drop_cols' : ['F4FN', 'appointed_date2', 'appointed_date3'],
        'loop_merge' : {
            'base_OD_edits' : [('appointed_date', ['appointed_date', 'appointed_date2', 'appointed_date3'])],
            'custom_merges' : [
                ['first_name_NS', 'last_name_NS', 'suffix_name', 'star'],
                {'query' : 'gender == "FEMALE"', 'cols' : ['first_name_NS', 'middle_initial', 'race', 'star', 'appointed_date']},
                {'query' : 'gender == "FEMALE"', 'cols' : ['first_name_NS', 'race', 'star', 'appointed_date']},
                {'query' : 'gender == "FEMALE"', 'cols' : ['F4FN', 'middle_initial', 'race', 'star', 'appointed_date']},
                {'query' : 'gender == "FEMALE"', 'cols' : ['F4FN', 'race', 'star', 'appointed_date']},
                ['first_name_NS', 'last_name_NS', 'race', 'gender', 'star']
            ],
            'mode' : 'otm',
            'cycle' : 'sup'
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
    .append_to_reference(inplace=True, drop_cols=cons.drop_cols)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
