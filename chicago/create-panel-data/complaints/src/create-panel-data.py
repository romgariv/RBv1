#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''create-master-data script for complaints'''
import __main__
import pandas as pd
import numpy as np

from setup import do_setup
from general_utils import FormatData, keep_duplicates

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/complaints-accused_2000-2016_2016-11.csv.gz',
        'input_cmpl_file' : 'input/complaints-complaints_2000-2016_2016-11.csv.gz',
        'input_witness_file': 'input/complaints-witnesses_2000-2016_2016-11.csv.gz',
        'input_OFC_file' : 'input/officer-filed-complaints__2017-09.csv.gz',
        'output_file' : 'output/monthly-panel_complaints_2000-2016.csv.gz',
        'output_outcomes_file' : 'output/monthly-panel_complaints-outcomes_2000-2016.csv.gz'
    }

    assert args['input_file'].startswith('input/'),\
        "Input_files are malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)



cons, log = get_setup()

# Monthly

acc = \
FormatData(log=log)\
    .import_data(cons.input_file)\
    .kcolumn(['cr_id', 'NUID', 'complaint_type', 'final_finding'])\
    .recode('final_finding', 'final_finding',
            {'sust' : ['SU', 'DIS'],
             'nonsust' : ['UN', 'NS', 'EX', 'NC'],
             'noaff' : 'NAF'})\
    .unique()\
    .data

assert keep_duplicates(acc, ['cr_id', 'NUID']).empty

cmpl = \
FormatData(log=log)\
    .import_data(cons.input_cmpl_file)\
    .kcolumn(['cr_id', 'incident_date', 'closed_date'])\
    .unique()\
    .date_to_month('incident_date', 'month')\
    .date_to_month('closed_date', 'closed_month')\
    .dcolumn(['incident_date', 'closed_date'])\
    .data

assert keep_duplicates(cmpl, 'cr_id').empty
assert not (set(acc.cr_id) - set(cmpl.cr_id))


acc = acc.merge(cmpl, on='cr_id', how='inner')
ofc = pd.read_csv(cons.input_OFC_file).cr_id
acc['cmpl_po'] = acc.cr_id.isin(ofc).astype(int)
acc['cmpl_civ'] = 1 - acc['cmpl_po']
acc['cmpl_FPS'] = (acc.complaint_type == 'FPS').astype(int)
acc['cmpl_Con'] = (acc.complaint_type.isin(['Search', 'UFV', 'ALU'])).astype(int)
acc['cmpl'] = 1

assert acc.NUID.isnull().sum() == 0
assert acc.cr_id.isnull().sum() == 0
assert keep_duplicates(acc, ['cr_id', 'NUID']).empty

log.info("Complaints between (months) %s and %s" % (acc.month.min(), acc.month.max()))
acc_sum = acc\
    .drop(['cr_id', 'complaint_type', 'final_finding', 'closed_month'], axis=1)\
    .groupby(['NUID', 'month'], as_index=False)\
    .sum()

log.info('Getting witness data')
wit = pd.read_csv(cons.input_witness_file)[['NUID', 'cr_id']]\
    .query('NUID == NUID')\
    .query('cr_id == cr_id')\
    .drop_duplicates()\
    .merge(cmpl[['cr_id', 'month']], on='cr_id', how='inner')
wit['was_witness'] = 1
wit = wit\
    .drop('cr_id', axis=1)\
    .groupby(['NUID', 'month'], as_index=False)\
    .sum()

log.info("Getting outcomes data")
acc = acc[['cr_id', 'NUID', 'closed_month', 'month', 'final_finding', 'complaint_type']]\
    .query('closed_month == closed_month')\
    .query('final_finding == final_finding')\
    .query('month <= closed_month')\
    .drop('month', axis=1)\
    .rename(columns={'closed_month' : 'month'})

for ff in acc.final_finding.unique():
    acc['cmpl_FPS_' + ff] = ((acc.complaint_type == 'FPS') & (acc.final_finding==ff)).astype(int)
    acc['cmpl_Con_' + ff] = (acc.complaint_type.isin(['Search', 'UFV', 'ALU']) & (acc.final_finding==ff)).astype(int)
    acc['cmpl_' + ff] = (acc.final_finding==ff).astype(int)
out_sum = acc\
    .drop(['cr_id', 'complaint_type', 'final_finding'], axis=1)\
    .groupby(['NUID', 'month'], as_index=False)\
    .sum()

cpanel = acc_sum.merge(wit, on=['NUID', 'month'], how='outer')\
    .dropna(subset=['NUID', 'month'], how='any')\
    .fillna(0)

log.info(("Total complaints in master data: N = %d, Civ = %d, PO = %d, "
          "Constitutional = %d, FPS = %d, was_witness = %d")
          % (cpanel.cmpl.sum(), cpanel.cmpl_civ.sum(), cpanel.cmpl_po.sum(),
             cpanel.cmpl_Con.sum(), cpanel.cmpl_FPS.sum(), cpanel.was_witness.sum()))
FormatData(cpanel, log=log).write_data(cons.output_file)

cpanel = acc_sum.merge(wit, on=['NUID', 'month'], how='outer')\
    .merge(out_sum, on=['NUID', 'month'], how='outer')\
    .dropna(subset=['NUID', 'month'], how='any')\
    .fillna(0)
FormatData(cpanel, log=log).write_data(cons.output_outcomes_file)

# Daily
log.info("Now creating Daily complaints")
acc = \
FormatData(log=log)\
    .import_data(cons.input_file)\
    .kcolumn(['cr_id', 'NUID', 'complaint_type', 'final_finding'])\
    .fillna('complaint_type', 'Missing')\
    .recode('final_finding', 'final_finding',
            {'sust' : ['SU', 'DIS'],
             'nonsust' : ['UN', 'NS', 'EX', 'NC'],
             'noaff' : 'NAF'})\
    .unique()\
    .data

assert keep_duplicates(acc, ['cr_id', 'NUID']).empty

cmpl = \
FormatData(log=log)\
    .import_data(cons.input_cmpl_file)\
    .kcolumn(['cr_id', 'incident_date'])\
    .unique()\
    .data

assert keep_duplicates(cmpl, 'cr_id').empty
assert not (set(acc.cr_id) - set(cmpl.cr_id))

acc = acc.merge(cmpl, on='cr_id', how='inner')
ofc = pd.read_csv(cons.input_OFC_file).cr_id
acc['civ'] = 1 - acc.cr_id.isin(ofc).astype(int)
acc['po'] = 1 - acc.civ

new_cols = []
for cmpl in acc.complaint_type.unique():
    new_col = "cmpl_" + cmpl
    acc.loc[(acc.complaint_type == cmpl), new_col] = 1
    acc[new_col] = acc[new_col].fillna(0)
    new_cols.append(new_col)

    for i in [0,1]:
        who = "po" if i == 0 else "civ"
        new_col = "cmpl_" + cmpl + "_" + who
        acc.loc[(acc.civ==i) & (acc.complaint_type == cmpl), new_col] = 1
        acc[new_col] = acc[new_col].fillna(0)
        new_cols.append(new_col)

FormatData(acc[['NUID', 'cr_id', 'incident_date', 'po', 'civ', ] + new_cols], log=log)\
    .write_data('output/compalints-clean_2000-2016_2016-11.csv.gz')
