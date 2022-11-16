#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for arrests_2001-2018_2018-12'''

import __main__
import pandas as pd
import numpy as np
from collections import OrderedDict

from setup import do_setup
from general_utils import FormatData, fill_data
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/arrests_2001-2018_2018-12.csv.gz',
        'output_file': 'output/arrests_2001-2018_2018-12.csv.gz',
        'intrafile_id': 'arrests_2001-2018_2018-12_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'uid': 'NUID',
        'add_cols' : [
            'F4FN', 'F4LN', 'L4LN', 'L4FN'
        ],
        'fill_cols' : [
            "first_name_NS", "last_name_NS", "middle_initial",
            "middle_initial2", "suffix_name", "appointed_date"
        ],
        'loop_merge' : {
            'base_OD_edits' : [
                ('first_name', ['first_name_NS', 'F4FN','L4FN']),
                ('last_name', ['last_name_NS', 'F4LN', 'L4LN'])
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

rfd = \
FormatData(log=log)\
    .import_data(cons.input_reference_file, uid=cons.uid, add_row_id=False)\
    .add_columns([
        {'exec' : ("_DATA_.loc[_DATA_['appointed_date'].isnull(), 'appointed_date']"
                     " = _DATA_.loc[_DATA_['appointed_date'].isnull(), ['start_date', 'org_hire_date']]"
                     ".apply(pd.to_datetime).apply(np.nanmax,axis=1).dt.date")},
        {'exec' : "_DATA_['appointed_date']=pd.to_datetime(_DATA_['appointed_date']).dt.date"}
    ])\
    .fill_data(cons.fill_cols)\
    .add_columns(cons.add_cols)

sfd = \
FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.intrafile_id)\
    .qfilter("first_name_NS==first_name_NS")\
    .add_columns([
        {'exec' : "_DATA_['appointed_date']=pd.to_datetime(_DATA_['appointed_date']).dt.date"}
    ])\
    .fill_data(cons.fill_cols)\
    .add_columns(cons.add_cols)

md = MergeData(rfd, sfd, log=log)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference(inplace=True, keep_um=False)\
    .remerge_to_file(cons.input_file, cons.output_file)

cons.write_yamlvar("conflict_NUID", md.conflicts.NUID.unique().tolist())
cons.write_yamlvar("conflict_arrests_2001-2018_2018-12_ID", md.conflicts["arrests_2001-2018_2018-12_ID"].unique().tolist())
