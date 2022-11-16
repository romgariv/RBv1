#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''create-panel-data script for salary'''

import __main__
import pandas as pd

from setup import do_setup
from general_utils import FormatData, keep_duplicates

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/salary_2002-2017_2017-09.csv.gz',
        'output_file' : 'output/monthly-panel_salary_2002-2017.csv.gz',
        'uid' : 'NUID',
        'min_start' : '2002-01-01',
        'max_end' : '2017-12-01'
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)



cons, log = get_setup()

def assign_id(x):
    x['ID'] = list(range(x.shape[0]))
    return x

sal = pd.read_csv(cons.input_file)[
    ['NUID', 'pay_grade', 'rank', 'salary', 'spp_date', 'year', 'employee_status']]

pan = pd.date_range(start=cons.min_start, end=cons.max_end, freq='MS')\
    .repeat(sal.NUID.nunique())\
    .to_frame()\
    .rename(columns={0:'month'})\
    .reset_index(drop=True)\
    .groupby('month', as_index=False)\
    .apply(assign_id)\
    .merge(pd.DataFrame({
        'NUID' : sal.NUID.unique(),
        'ID' : list(range(sal.NUID.nunique()))
        }),
        on='ID', how='inner')\
    .drop('ID', axis=1)

pan['year'] = pan['month'].dt.year

st = sal[['NUID', 'pay_grade', 'rank', 'spp_date']].dropna(subset=['spp_date'])
st.insert(0, 'month', pd.to_datetime(st['spp_date'].map(lambda x: x[:-2] + '01')))
st = st\
    .sort_values(['NUID', 'spp_date', 'pay_grade'])\
    .groupby(['NUID', 'month'], as_index=False)\
    .last()

sp = st[st['spp_date'] < '2002-02-01']\
    .drop('month', axis=1)\
    .groupby('NUID', as_index=False).last()
sp.insert(0, 'month', pd.to_datetime('2002-01-01'))

st = st[st['spp_date'] >= '2002-02-01']\
    .append(sp, sort=False)\
    .sort_values(['NUID', 'spp_date', 'pay_grade'])\
    .drop('spp_date', axis=1)\
    .drop_duplicates()\
    .reset_index(drop=True)
st['gen'] = 0

assert keep_duplicates(st, ['NUID', 'month']).empty

sn = sal.loc[sal['spp_date'].isnull(), ['NUID', 'pay_grade', 'rank', 'year']]
sn['month'] = pd.to_datetime(sn['year'].map(lambda x: str(x) + '-01-01'))
sn = sn\
    .sort_values(['NUID', 'month', 'pay_grade'])\
    .groupby(['NUID', 'month'], as_index=False)\
    .last()
sn['gen'] = 1

st = st.append(sn, sort=False)\
    .drop('year', axis=1)\
    .drop_duplicates()\
    .sort_values(['NUID', 'month', 'gen', 'pay_grade'])\
    .groupby(['NUID', 'month'], as_index=False)\
    .first()\
    .reset_index(drop=True)\
    .drop('gen', axis=1)

assert keep_duplicates(st, ['NUID', 'month']).empty

pan = pan.merge(st, how='left', on=['NUID', 'month'])
nuids = pan.NUID
pan = pan.groupby('NUID').fillna(method='ffill')
pan['NUID'] = nuids

pan = pan.merge(
    sal[['NUID', 'year']].drop_duplicates(),
    on=['NUID','year'],
    how='inner')
pan = pan.merge(sal, on=['NUID', 'year', 'rank', 'pay_grade'], how='left')

assert keep_duplicates(pan, ['NUID', 'month']).empty

FormatData(pan, log=log).write_data(cons.output_file)
