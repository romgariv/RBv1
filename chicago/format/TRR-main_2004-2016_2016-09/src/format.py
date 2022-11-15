#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for TRR-main_2004-2016_2016-09'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/10655-FOIA-P046360-TRRdata_sterilized.xlsx',
        'output_file' : 'output/TRR-main_2004-2016_2016-09.csv.gz',
        'sheets' : ['Sheet1', 'Star #'],
        'column_names_key': 'TRR-main_2004-2016_2016-09',
        'ID' : 'TRR-officers_2004-2016_2016-09_ID',
        'auid_args' : {
            'id_cols': [
                'first_name', 'last_name', 'first_name_NS', 'last_name_NS',
                'middle_initial', 'middle_initial2', 'suffix_name',
                'appointed_date', 'gender', 'race', 'current_star'
                ]
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
    .import_data(cons.input_file, column_names=cons.column_names_key,
                 sheets=cons.sheets)\
    .dcolumn(['ln_drop', 'fn_drop'])\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .write_data(cons.output_file)
