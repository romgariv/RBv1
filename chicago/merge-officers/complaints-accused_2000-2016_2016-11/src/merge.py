#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for complaints-accused_2000-2016_2016-11'''

import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/complaints-accused_2000-2016_2016-11.csv.gz',
        'output_file': 'output/complaints-accused_2000-2016_2016-11.csv.gz',
        'intrafile_id': 'complaints-accused_2000-2016_2016-11_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'agg_args' : {
            'id_cols': [
                'first_name', 'last_name', 'suffix_name',
                'first_name_NS', 'last_name_NS',
                'appointed_date', 'birth_year', 'gender', 'race',
                ],
            'max_cols': [
                'middle_initial', 'middle_initial2',
                'current_unit', 'current_star', 'current_rank'
                ],
            },
        'radd_cols' : ['F4FN', 'F4LN'],
        'sadd_cols' : ['F4FN', 'F4LN', '_DATA_["star"]=_DATA_["current_star"]'],
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
    .aggregate(cons.agg_args)\
    .add_columns(cons.sadd_cols)

MergeData(rfd, sfd, log=log)\
    .loop_merge()\
    .append_to_reference(inplace=True, drop_cols=cons.drop_cols)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
