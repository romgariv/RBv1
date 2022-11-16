#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for complaints-complaints_2000-2018_2018-07'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/case_info_export.xlsx',
        'output_file': 'output/complaints-complaints_2000-2018_2018-07.csv.gz',
        'column_names_key': 'complaints-complaints_2000-2018_2018-07',

    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

FormatData(log=log)\
    .import_data(cons.input_file, column_names=cons.column_names_key)\
    .clean()\
    .write_data(cons.output_file)
