#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for TRR-weapon-discharges_2004-2018_2018-08'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/P456008 13094-FOIA-P456008-TRRdata Responsive Record Produced By R&A_sterilized.xlsx',
        'output_file' : 'output/TRR-weapon-discharges_2004-2018_2018-08.csv.gz',
        'sheet' : 'WeaponDischarges',
        'column_names_key': 'TRR-weapon-discharges_2004-2018_2018-08'
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
                 sheets=cons.sheet)\
    .clean()\
    .write_data(cons.output_file)
