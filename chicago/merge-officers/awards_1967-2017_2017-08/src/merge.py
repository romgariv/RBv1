#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for awards_1967-2017_2017-08'''
import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/awards_1967-2017_2017-08.csv.gz',
        'output_file': 'output/awards_1967-2017_2017-08.csv.gz',
        'intrafile_id': 'awards_1967-2017_2017-08_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'qfilter' : 'likely_sworn_officer == 1',
        'drop_cols' : [
            'ROWID', 'pps_award_detail_id', 'award_type', 'ceremony_date',
            'award_request_date', 'rd_no', 'current_award_status',
            'requester_full_name', 'award_start_date', 'award_end_date',
            'likely_sworn_officer', 'awards-requester_1967-2017_2017-08_ID'
                       ],
        'radd_cols' : [
            'F4FN', 'F4LN', 'F2FN'],
        'sadd_cols' : [
            'F4FN', 'F4LN', 'F2FN', {'exec' : '_DATA_["star"]=_DATA_["current_star"]'}],
        'drop_cols2' : ['ROWID', 'F4FN', 'F4LN', 'F2FN'],
        'loop_merge' : {
            'custom_merges' : [
                {'query' : 'gender=="FEMALE"',
                 'cols' : ['first_name_NS', 'star', 'appointed_date', 'birth_year', 'gender', 'race']},
                ['F2FN', 'last_name_NS', 'star', 'appointed_date', 'birth_year'],
                {'query' : 'gender=="FEMALE"',
                 'cols' : ['first_name_NS', 'middle_initial', 'appointed_date', 'birth_year', 'gender', 'race']},
                ['first_name_NS', 'last_name_NS', 'birth_year', 'star', 'race', 'middle_initial'],
                ['first_name_NS', 'last_name_NS', 'birth_year', 'middle_initial', 'race', 'gender'],
                ['first_name_NS', 'last_name_NS', 'middle_initial', 'appointed_date'],
                ]
                },
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
    .dcolumn(cons.drop_cols)\
    .unique()\
    .add_columns(cons.sadd_cols)

MergeData(rfd, sfd, log=log)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference(inplace=True, drop_cols=cons.drop_cols2)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)