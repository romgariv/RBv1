#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''create-master-data script for arrests'''
import __main__
import pandas as pd
import numpy as np

from setup import do_setup
from general_utils import FormatData, keep_duplicates

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/arrest-info_2001-2017_2017-07.csv.gz',
        'input_AO_file' : 'input/arrests_2001-2018_2018-12.csv.gz',
        'output_file' : 'output/monthly-panel_arrests_2010-2017.csv.gz',
        'output_released_file' : 'output/monthly-panel_arrests-released_2007-2017.csv.gz',
        'output_min_file' : 'output/arrests-mindate_2001-2017.csv.gz'
    }

    assert args['input_file'].startswith('input/'),\
        "Input_files are malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

#### Arrest Date

ai = FormatData(log=log)\
    .import_data(cons.input_file)\
    .qfilter('year >= 2010')\
    .kcolumn(['arrest_date', 'crime_code', 'cb_no', 'year'])\
    .date_to_month('arrest_date', 'month')\
    .qfilter('month == month')\
    .qfilter('cb_no == cb_no & year == year')\
    .generate_dummy_cols('crime_code', 'arrest_')\
    .map('arrest_any', 1)\
    .dcolumn(['arrest_date', 'crime_code'])\
    .data

assert keep_duplicates(ai, ['cb_no', 'year']).empty

ao = FormatData(log=log)\
    .import_data(cons.input_AO_file)\
    .kcolumn(['NUID', 'cb_no', 'year'])\
    .qfilter('year >= 2010')\
    .qfilter('NUID == NUID')\
    .qfilter('cb_no == cb_no & year == year')\
    .data

assert keep_duplicates(ao, ['cb_no', 'NUID', 'year']).empty

an = ao.merge(ai, how='inner', on=['cb_no', 'year'])
log.info("%d CBs matched out of %d in info and %d in officers"
         % (an.cb_no.nunique(), ai.cb_no.nunique(), ao.cb_no.nunique()))
del an['year']
del an['cb_no']
an = an\
    .groupby(['NUID', 'month'], as_index=False)\
    .sum()

FormatData(an, log=log).write_data(cons.output_file)

del ao, ai, an

#### Release Date

ai = FormatData(log=log)\
    .import_data(cons.input_file)\
    .qfilter('year >= 2007')\
    .kcolumn(['released_date', 'crime_code', 'cb_no', 'year'])\
    .date_to_month('released_date', 'month')\
    .qfilter('month == month')\
    .qfilter('cb_no == cb_no & year == year')\
    .generate_dummy_cols('crime_code', 'released_')\
    .map('released_any', 1)\
    .dcolumn(['released_date', 'crime_code'])\
    .data

assert keep_duplicates(ai, ['cb_no', 'year']).empty

ao = FormatData(log=log)\
    .import_data(cons.input_AO_file)\
    .kcolumn(['NUID', 'cb_no', 'year'])\
    .qfilter('year >= 2007')\
    .qfilter('NUID == NUID')\
    .qfilter('cb_no == cb_no & year == year')\
    .data

assert keep_duplicates(ao, ['cb_no', 'NUID', 'year']).empty

an = ao.merge(ai, how='inner', on=['cb_no', 'year'])
log.info("%d CBs matched out of %d in info and %d in officers"
         % (an.cb_no.nunique(), ai.cb_no.nunique(), ao.cb_no.nunique()))
del an['year']
del an['cb_no']
an = an\
    .groupby(['NUID', 'month'], as_index=False)\
    .sum()

FormatData(an, log=log).write_data(cons.output_released_file)

del ao, ai, an


#### Unified Data
ai = FormatData(log=log)\
    .import_data(cons.input_file)\
    .kcolumn(['arrest_date', 'released_date', 'bond_date', 'crime_code', 'cb_no', 'year'])\
    .qfilter('cb_no == cb_no & year == year')\
    .data

for c in ['arrest_date', 'released_date', 'bond_date']:
    ai[c] = pd.to_datetime(ai[c], errors='coerce').dt.date

ai['date'] = ai['arrest_date']
ai['min_date'] = ai[['released_date', 'bond_date']].apply(pd.to_datetime).apply(np.nanmin, axis=1).dt.date
ai.loc[ai.date.isnull(), 'date'] = ai.loc[ai.date.isnull(), 'min_date']

assert keep_duplicates(ai, ['cb_no', 'year']).empty

ai = ai[['cb_no', 'crime_code', 'arrest_date', 'date', 'year']]

ao = FormatData(log=log)\
    .import_data(cons.input_AO_file)\
    .kcolumn(['NUID', 'cb_no', 'year'])\
    .qfilter('NUID == NUID')\
    .qfilter('cb_no == cb_no & year == year')\
    .data

assert keep_duplicates(ao, ['cb_no', 'NUID', 'year']).empty

FormatData(ai.merge(ao, on=['cb_no', 'year']), log=log)\
    .write_data(cons.output_min_file)
