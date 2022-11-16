#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for evaluations-supervisors_2009-2017_2017-04'''

import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/evaluations-supervisors_2009-2017_2017-04.csv.gz',
        'output_file': 'output/evaluations-supervisors_2009-2017_2017-04.csv.gz',
        'intrafile_id': 'evaluations-supervisors_2009-2017_2017-04_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'profile_cols' : [
            'first_name', 'last_name', 'first_name_NS', 'last_name_NS',
            'birth_year', 'race', 'current_rank', 'current_unit',
            'middle_initial', 'suffix_name', 'middle_initial2',
            ],
        'qfilter1' : 'first_name_NS != "OFFICER"',
        'qfilter2':  "current_rank not in ['ADMIN. MANAGER', 'DIRECTOR OF FACILITIES', 'SUPV INVEN CONT 1', 'PROJECT MANAGER', 'MEDICAL ADMIN.', 'POLICE ADMINISTRATIVE CLERK', 'MGR POLICE PERSONNEL', 'DEPUTY DIRECTOR', 'DIR RESEARCH/PLANING']",
        'radd_cols' : ['F4FN', 'F4LN'],
        'sadd_cols' : ['F4FN', 'F4LN'],
        'drop_cols' : ['F4FN', 'F4LN'],
        'loop_merge' : {
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
    .qfilter(cons.qfilter1)\
    .qfilter(cons.qfilter2)\
    .unique([cons.intrafile_id] + cons.profile_cols)\
    .add_columns(cons.sadd_cols)

MergeData(rfd, sfd, log=log)\
    .loop_merge()\
    .append_to_reference(inplace=True, drop_cols=cons.drop_cols)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
