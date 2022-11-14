#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for awards_1967-2017_2017-08'''

import __main__
import yaml
import pandas as pd

from general_utils import FormatData
from setup import do_setup

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/Awards_Data_(New_Copy).csv.gz',
        'output_file': 'output/awards_1967-2017_2017-08.csv.gz',
        'column_names_key': 'awards_1967-2017_2017-08',
        'ID': 'awards_1967-2017_2017-08_ID',
        'auid_args' : {
            'id_cols': [
                "first_name_NS", "last_name_NS"
                ],
            'conflict_cols': [
                'appointed_date', 'gender', 'race',
                'middle_initial', 'middle_initial2',
                'birth_year', 'current_star', 'resignation_date'
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

with open("hand/award_po_ranks.yaml", "r") as f:
    po_ranks = yaml.load(f)
with open("hand/maybe_po_ranks.yaml", "r") as f:
    maybe_po_ranks = yaml.load(f)

fd = \
FormatData(log=log)\
    .import_data(cons.input_file, add_row_id=True,
                 column_names=cons.column_names_key)\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)

df = fd.data

log.info('Replacing award_request_date="2014-11-19" if award_request_date=="2013-11-19" & award_type=="EMBLEM OF RECOGNITION - PHYSICAL FITNESS"')
df.loc[(df["award_request_date"] == "2013-11-19") &
       (df["award_type"] == "EMBLEM OF RECOGNITION - PHYSICAL FITNESS"),
       "award_request_date"] = "2014-11-19"

with open("hand/award_po_ranks.yaml", "r") as f:
    po_ranks = yaml.load(f)
with open("hand/maybe_po_ranks.yaml", "r") as f:
    maybe_po_ranks = yaml.load(f)

po_ids = df.loc[(df['rank'].isin(po_ranks)) |
                ((df['rank'].isin(maybe_po_ranks)) &
                 (pd.to_datetime(df['appointed_date']) < "2010-01-01")),
                cons.ID].unique()
df['likely_sworn_officer'] = df[cons.ID].isin(po_ids).astype(int)
log.info('%d officers in file' % df.loc[df['likely_sworn_officer']==1, cons.ID].nunique())

log.info("Cleaning supervisors data")
req = FormatData(df[['ROWID', 'award_request_date', 'requester_full_name']], log=log)\
    .set_columns({'requester_full_name' : 'full_name'})\
    .clean()\
    .qfilter("first_name == first_name")\
    .assign_unique_ids('awards-requester_1967-2017_2017-08_ID',
                       {'id_cols' : ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name']})\
    .write_data('output/awards-requester_1967-2017_2017-08.csv.gz')

df = df.merge(req.data[['ROWID', 'awards-requester_1967-2017_2017-08_ID']], on='ROWID', how='left')

FormatData(df, log=log, uid=cons.ID)\
    .LOG('Added "likely_sworn_officer" column base on rank and appointed_date')\
    .write_data(cons.output_file)



