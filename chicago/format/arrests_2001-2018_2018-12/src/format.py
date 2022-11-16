#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for arrests_2001-2018_2018-12'''

import __main__
import pandas as pd
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_files': [
            "input/13053 PAC Appeal- Officer's Information Requested 2001.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2002.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2003.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2004.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2005.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2006.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2007.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2008.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2009.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2010.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2011.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2012.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2013.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2014.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2015.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2016.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2017.xlsx",
            "input/13053 PAC Appeal- Officer's Information Requested 2018.xlsx"
            ],
        'output_file': 'output/arrests_2001-2018_2018-12.csv.gz',
        'column_names_key': 'arrests_2001-2018_2018-12',
        'ID' : 'arrests_2001-2018_2018-12_ID'
        }

    assert all(input_file.startswith('input/')
               for input_file in args['input_files']),\
        "An input_file is malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()
full_data = pd.DataFrame()
for in_file in cons.input_files:
    year = int(in_file[-9:-5])
    full_data = full_data.append(
        FormatData(log=log)\
            .import_data(in_file, column_names=cons.column_names_key, skiprows=3)\
            .copy_col('human_name', 'stored_name')\
            .clean()\
            .add_columns([{'exec' : '_DATA_["year"] = {}'.format(year)}])\
            .data
    )

FormatData(full_data, log=log)\
    .assign_unique_ids(
        cons.ID.format(year),
        {'id_cols': ['first_name_NS', 'last_name_NS', 'appointed_date'],
         'conflict_cols' : ['middle_initial', 'middle_initial2', 'suffix_name']}
    )\
    .map('cb_no', lambda x: pd.to_numeric(x, errors='coerce'))\
    .qfilter('cb_no==cb_no')\
    .write_data(cons.output_file)
