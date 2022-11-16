#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for TRR-subject-weapons_2004-2016_2016-09'''

import __main__
import pandas as pd
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/10655-FOIA-P046360-TRRdata_sterilized.xlsx',
        'output_file' : 'output/TRR-subject-weapons_2004-2016_2016-09.csv.gz',
        'sheet' : 'SubjectWeapons',
        'column_names_key': 'TRR-subject-weapons_2004-2016_2016-09'
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()
df = \
FormatData(log=log)\
    .import_data(cons.input_file, column_names=cons.column_names_key,
                 sheets=cons.sheet, add_row_id=False)\
    .data

log.info("Parsing columns with _x000D_\\n due to error in original file")
er1 = df.loc[25287, 'weapon_description']
new_rows1 = er1.split('_x000D_\n')
df.loc[25287, 'weapon_description'] = new_rows1[0]
df = df.append(pd.DataFrame([i.split('\t') for i in new_rows1[1:]],
                            columns=df.columns))
er2 = df.loc[47336, 'weapon_description']
new_rows2 = er2.split('_x000D_\n')
new_rows2[-1] = new_rows2[-1] + df.loc[47337, 'trr_id'] + '\t\t'
df = df.drop(47337)
df.loc[47336, 'weapon_description'] = new_rows2[0]
df = df.append(pd.DataFrame([i.split('\t') for i in new_rows2[1:]],
               columns=df.columns))

df.reset_index(drop=True, inplace=True)
df.insert(0, 'ROWID', df.index + 1)

FormatData(df, log=log)\
    .clean()\
    .write_data(cons.output_file)
