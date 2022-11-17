#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for officer-filed-complaints__2017-09'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/tabula-FOIA_P428703_Responsive_Records.csv.gz',
        'output_file': 'output/officer-filed-complaints__2017-09.csv.gz',
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

FormatData(log=log)\
    .import_data(cons.input_file, no_header=True, strip_colnames=False, add_row_id=False)\
    .set_columns(['cr_id'])\
    .write_data(cons.output_file)
