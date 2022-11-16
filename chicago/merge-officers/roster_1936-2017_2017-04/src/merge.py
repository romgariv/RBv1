#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for roster_1936-2017_2017-04'''
import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/roster_1936-2017_2017-04.csv.gz',
        'output_file': 'output/roster_1936-2017_2017-04.csv.gz',
        'intrafile_id': 'roster_1936-2017_2017-04_ID',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'qfilter' : 'first_name != "POLICE" & first_name == first_name',
        'agg_args' : {
            'id_cols': [
                'roster_1936-2017_2017-04_ID', "first_name_NS", "last_name_NS",
                'suffix_name', "appointed_date", "gender",
                'birth_year', 'current_age', 'resignation_date', 'current_rank'
                ],
            'max_cols': [
                'middle_initial', 'middle_initial2',
                'star1', 'star2', 'star3', 'star4', 'star5',
                'star6', 'star7', 'star8', 'star9', 'star10',
                'current_status', 'current_unit', 'race'],
            'merge_cols': ['unit_description'],
            'merge_on_cols': ['current_unit'],
            }
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.intrafile_id)\
    .reuid(cons.uid, skip_query=cons.qfilter)\
    .write_data(cons.output_file)\
    .qfilter(cons.qfilter)\
    .aggregate(cons.agg_args)\
    .reshape_long('star')\
    .write_data(cons.output_reference_file)
