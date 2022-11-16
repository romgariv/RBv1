#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for complaints-investigators_2000-2016_2016-11'''

import __main__
import pandas as pd
import numpy as np

from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/complaints-investigators_2000-2016_2016-11.csv.gz',
        'new_inv_file': 'input/complaints-investigators_2000-2018_2018-03.csv.gz',
        'output_file': 'output/complaints-investigators_2000-2016_2016-11.csv.gz',
        'intrafile_id': 'complaints-investigators_2000-2016_2016-11_ID',
        'inv_ipra_id': 'complaints-IPRA-investigators_2000-2016_2016-11_ID',
        'new_inv_intrafile_id': 'complaints-investigators_2000-2018_2018-03_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file' : 'output/officer-reference.csv.gz',
        'output_inv_file': 'output/complaints-IPRA-investigators_2000-2016_2016-11.csv.gz',
        'inv_ranks_file' : 'hand/investigator_ranks.csv',
        'uid': 'NUID',
        'qfilter_po' : 'rank_type == "PO"',
        'qfilter_inv' : 'rank_type == "IPRA"',
        'po_profile_cols' : [
            'first_name_NS', 'last_name_NS', 'appointed_date',
            'suffix_name', 'middle_initial', 'current_unit'
        ],
        'radd_cols' : ['F4FN', 'F4LN'],
        'sadd_cols' : ['F4FN', 'F4LN'],
        'drop_cols' : ['F4FN', 'F4LN'],
        'inv_loop_merge' : {
                'custom_merges' : [
                    ['first_name_NS', 'last_name_NS', 'current_unit'],
                    ['first_name_NS', 'last_name_NS']
                ],
                'mode' : 'otm'
        },
        'agg_args' : {
            'mode_cols' : [
                'race', 'gender', 'current_unit', 'current_rank',
                'birth_year', 'appointed_date'
                ],
            'max_cols' : ['complaints-investigators_2000-2016_2016-11_ID'],
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
    .add_columns(cons.radd_cols)\
    .add_columns([
        {'exec' : ("_DATA_.loc[_DATA_['appointed_date'].isnull(), 'appointed_date']"
                     " = _DATA_.loc[_DATA_['appointed_date'].isnull(), ['start_date', 'org_hire_date']]"
                     ".apply(pd.to_datetime).max(axis=1).dt.date")}
    ])

sfd = \
FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.intrafile_id)

log.info("Adding rank types and cleaned ranks from %s" % cons.inv_ranks_file)
ranks = pd.read_csv(cons.inv_ranks_file)\
    .rename(columns={'rank' : 'current_rank'})

sdf = sfd.data.merge(ranks, on='current_rank', how='left')
log.info('PO = %d, IPRA = %d. OTHER = %d' %
         ((sdf['rank_type']=='PO').sum(),
          (sdf['rank_type']=='IPRA').sum(),
          (sdf['rank_type']=='OTHER').sum()))

sfd_po = FormatData(sdf, log=log, uid=cons.intrafile_id)\
    .qfilter(cons.qfilter_po)\
    .unique([cons.intrafile_id] + cons.po_profile_cols)\
    .add_columns(cons.sadd_cols)\
    .add_columns([
        {'exec' : "_DATA_.appointed_date=pd.to_datetime(_DATA_.appointed_date).dt.date"}
    ])

md = \
MergeData(rfd, sfd_po, log=log)\
    .loop_merge()\
    .append_to_reference(
        inplace=True, drop_cols=cons.drop_cols, keep_um=False)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .add_columns([{'exec' : '_DATA_.loc[_DATA_["start_date"].notnull() | _DATA_["org_hire_date"].notnull(), "appointed_date"]=np.nan'}])\
    .write_data(cons.output_reference_file)

# sfd_inv = FormatData(sdf, log=log, uid=cons.intrafile_id)\
#     .qfilter(cons.qfilter_inv)\
#     .reuid(cons.inv_ipra_id)\
#     .add_columns(cons.sadd_cols)

# new_inv = FormatData(log=log, uid=cons.new_inv_intrafile_id)\
#     .import_data(cons.new_inv_file)\
#     .add_columns(cons.sadd_cols)

# md2 = MergeData(sfd_inv, new_inv, log=log)\
#     .loop_merge(**cons.inv_loop_merge)\
#     .append_to_reference(inplace=True, keep_um=False)\
#     .to_FormatData\
#     .aggregate(cons.agg_args)
# ninv = md2.data
# assert set(ninv[cons.intrafile_id]) == set(sdf.query('rank_type == "IPRA"')[cons.intrafile_id])
# ninv = ninv.merge(sdf[[cons.intrafile_id, 'cr_id']], on=cons.intrafile_id)
# assert set(ninv[cons.intrafile_id]) == set(sdf.query('rank_type == "IPRA"')[cons.intrafile_id])
# FormatData(ninv, log=log).write_data(cons.output_inv_file)
