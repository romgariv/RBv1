#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''officer-profiles generation script'''

import pandas as pd
import numpy as np
import yaml
import __main__


from general_utils import FormatData
from setup import do_setup


def get_setup():
    ''' encapsulates args.
        calls setup.do_setup() which returns constants and logger
        constants contains args and a few often-useful bits in it
        including constants.write_yamlvar()
        logger is used to write logging messages
    '''
    script_path = __main__.__file__
    args = {
        'input_file': 'input/officer-reference.csv.gz',
        'output_file': 'output/officer-profiles.csv.gz',
        'universal_id': 'NUID',
        'column_order': [
            'first_name', 'first_name_NS',
            'last_name', 'last_name_NS',
            'middle_initial', 'middle_initial2', 'suffix_name',
            'birth_year', 'race', 'gender',
            'appointed_date', 'resignation_date',
            'start_date', 'org_hire_date', 'current_rank'
            ],
        'aggregate_data_args' : {
            'mode_cols': [
                'first_name', 'last_name', 'middle_initial', 'first_name_NS', 'last_name_NS',
                'middle_initial2', 'suffix_name', 'race', 'gender',
                'birth_year', 'appointed_date', 'start_date', 'org_hire_date'
                ],
            'max_cols': ['resignation_date'],
            'time_col' : 'FOIA_date',
            'current_cols': ['current_rank']
            }
        }

    assert args['input_file'] == 'input/officer-reference.csv.gz',\
        'Input file is not correct.'
    assert args['output_file'] == 'output/officer-profiles.csv.gz',\
        'Output file is not correct.'

    return do_setup(script_path, args)


cons, log = get_setup()

FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.universal_id)\
    .add_columns([
    {'exec' : '_DATA_["FOIA_date"] = pd.NaT'},
    {'exec' :
     ('for col in [col for col in _DATA_.columns if (col.endswith("_ID") and ("WHEN" not in col))]:'
'_DATA_.loc[_DATA_[col].notnull(), "FOIA_date"] = (col[-10:-3] + "-01")')}
])\
    .aggregate(cons.aggregate_data_args)\
    .kcolumn(cons.column_order)\
    .add_columns([
        {'exec': '_DATA_["current_rank"] = _DATA_["current_rank"].str.upper()'}
    ])\
    .write_data(cons.output_file)
