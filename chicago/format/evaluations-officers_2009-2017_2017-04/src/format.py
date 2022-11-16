#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for evaluations-officers_2009-2017_2017-04'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/cpd_officers_170313 NEW.xls',
        'output_file' : 'output/evaluations-officers_2009-2017_2017-04.csv.gz',
        'keep_columns': [
            'EVAL_YEAR', 'FIRST_NAME_O', 'MI_O', 'LAST_NAME_O',
            'ASSIGNED_O', 'SEX_O', 'BIRTH_O', 'RACE_O', 'CURRENT_TITLE_O',
            'UNIT_O', 'EVAL_UNIT_O'
            ],
        'column_names_key' : 'evaluations-officers_2009-2017_2017-04',
        'ID' : 'evaluations-officers_2009-2017_2017-04_ID',
        'auid_args' : {
                'id_cols' : [
                    'first_name_NS', 'last_name_NS',
                    'current_unit', 'birth_year'
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
    .import_data(cons.input_file,
                 sheets=[0,1], concat_axis=0,
                 keep_columns=cons.keep_columns,
                 column_names=cons.column_names_key)\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .write_data(cons.output_file)
