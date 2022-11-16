#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for complaints-CPD-witnesses_2000-2018_2018-07'''
import __main__
import yaml
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/cpd_witness_export.xlsx',
        'output_file': 'output/complaints-CPD-witnesses_2000-2018_2018-07.csv.gz',
        'column_names_key': 'complaints-CPD-witnesses_2000-2018_2018-07',
        'ID': 'complaints-CPD-witnesses_2000-2018_2018-07_ID',
        'auid_args' : {
            'id_cols': ['employee_id']
        }
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)



cons, log = get_setup()
fd = \
FormatData(log=log)\
    .import_data(cons.input_file,
                 column_names=cons.column_names_key)\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)

df = fd.data
assert df.employee_id.notnull().all()

with open("hand/nc_nonpo_ranks.yaml", "r") as f:
    nonpo_ranks = yaml.load(f)

po_ids = df.loc[(df['rank'].notnull()) &
                (~df['rank'].isin(nonpo_ranks)),
                cons.ID].unique().tolist()
df['likely_sworn_officer'] = (df[cons.ID].isin(po_ids)).astype(int)
log.info('%d officers in file' % df.loc[df['likely_sworn_officer']==1, cons.ID].nunique())
FormatData(df, log=log, uid=cons.ID)\
    .LOG('Added "likely_sworn_officer" column base on rank')\
    .write_data(cons.output_file)