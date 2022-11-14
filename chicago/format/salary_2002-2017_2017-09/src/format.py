    #!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for salary_2002-2017_2017-09'''

import __main__
import pandas as pd
import numpy as np
from setup import do_setup
from general_utils import FormatData, standardize_columns
from assign_unique_ids_functions import assign_unique_ids
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_files': [
            'input/9-13-17_FOIA_Updated_CPD_Data-2002-2007.xls',
            'input/9-13-17_FOIA_Updated_CPD_Data-2008-2012.xls',
            'input/9-13-17_FOIA_Updated_CPD_Data_-2013-2017.xls'
            ],
        'output_file': 'output/salary_2002-2017_2017-09.csv.gz',
        'output_profiles_file': 'output/salary_2002-2017_2017-09_profiles.csv.gz',
        'column_names_key': 'salary_2002-2017_2017-09',
        'ID' : 'salary_2002-2017_2017-09_ID',

        'id_cols': [
            'first_name_NS', 'last_name_NS',
            'middle_initial', 'middle_initial2',
            'suffix_name', 'gender'
            ],
        'sub_id_cols': [
            'first_name_NS', 'last_name_NS',
            'middle_initial', 'middle_initial2',
            'suffix_name', 'gender',
             'year'
            ],
        'profile_cols' : [
            'salary_2002-2017_2017-09_ID', 'age_at_hire', 'gender',
            'first_name', 'last_name',
            'first_name_NS', 'last_name_NS', 'suffix_name',
            'middle_initial', 'middle_initial2', 'so_max_year', 'so_min_year',
            'so_max_year_m1', 'so_min_year_m1',
            'so_max_date', 'so_min_date', 'current_age_p1', 'current_age2_pm',
            'current_age_m1', 'current_age_pm', 'current_age_mp',
             'current_age2_p1','current_age2_m1','current_age2_mp',
            'start_date', 'org_hire_date', 'resignation_year'
        ],
        'sub_conflict_cols': ['org_hire_date', 'age_at_hire'],
        'ID': 'salary_2002-2017_2017-09_ID',
        'sub_id': 'salary-year_2002-2017_2017-09_ID',
        'ad_cols': ['start_date', 'org_hire_date'],
        'year_id': 'salary-year_2002-2017_2017-09_ID',
        'custom_merges': [
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'suffix_name','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'suffix_name', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'suffix_name','org_hire_date', 'age_at_hire'],
            ['F4FN', 'F4LN', 'suffix_name','org_hire_date', 'start_date', 'age_at_hire'],
            ['F4FN', 'F4LN', 'suffix_name', 'start_date', 'age_at_hire'],
            ['F4FN', 'F4LN', 'suffix_name','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS','start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS','org_hire_date', 'age_at_hire'],
            ['first_name_NS', 'middle_initial', 'suffix_name','org_hire_date', 'start_date', 'age_at_hire'],
            ['first_name_NS', 'last_name_NS', 'org_hire_date', 'spp_date', 'salary', 'pay_grade'],
            ['first_name_NS', 'last_name_NS', 'spp_date','org_hire_date', 'start_date', 'pay_grade'],
            ['first_name_NS', 'last_name_NS', 'spp_date','org_hire_date', 'pay_grade'],
            ['first_name_NS', 'last_name_NS', 'spp_date','start_date', 'pay_grade'],
            ['first_name_NS', 'last_name_NS', 'org_hire_date'],
            ['first_name_NS', 'last_name_NS', 'start_date'],
            ['first_name_NS', 'middle_initial', 'spp_date', 'start_date', 'org_hire_date', 'pay_grade'],
            ['first_name_NS', 'spp_date', 'start_date', 'org_hire_date', 'pay_grade', 'age_at_hire']
        ]
        }

    assert all(input_file.startswith('input/')
               for input_file in args['input_files']),\
        "An input_file is malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

data_df = pd.DataFrame()

for input_file in cons.input_files:
    xls_file = pd.ExcelFile(input_file)
    for sheet in xls_file.sheet_names:
        df = xls_file.parse(sheet)
        df.columns = standardize_columns(df.columns, cons.column_names_key)
        df.insert(0, 'year', int(sheet))
        data_df = (data_df
                   .append(df)
                   .reset_index(drop=True))

df = FormatData(data_df, log=log)\
    .clean()\
    .data

full_df = pd.DataFrame()

for year in df['year'].unique():
    log.info('Assigning unique sub-ids for year: %d', year)
    sub_df = df[df['year']==year]
    sub_df = assign_unique_ids(sub_df, cons.sub_id,
                               cons.sub_id_cols,
                               conflict_cols=cons.sub_conflict_cols,
                               log=log)
    sub_df[cons.sub_id] = sub_df[cons.sub_id] + year * 100000
    full_df = full_df.append(sub_df)

assert full_df.shape[0] == df.shape[0],\
    print('Remerged data does not match input dataset')

df = full_df

log.info("Beginning self-merge process")

for year in range(2002, 2018):
    print(year)
    dfy = FormatData(df[df['year'] == year].copy(),
                         log=log, uid = cons.year_id)\
    .set_columns({cons.year_id: cons.year_id.replace('year', str(year))})\
    .add_columns(['F4FN', 'F4LN'])


    if year == 2017:
        MD = MergeData(ref=(MD.to_FormatData.add_columns([
                       {'exec' : '_DATA_["age_at_hire_p1"] = _DATA_["age_at_hire"]'},
                       {'exec' : '_DATA_["age_at_hire_m1"] = _DATA_["age_at_hire"]'}])),
                  sup=(dfy.add_columns([
                       {'exec' : '_DATA_["age_at_hire_p1"] = _DATA_["age_at_hire"] + 1'},
                       {'exec' : '_DATA_["age_at_hire_m1"] = _DATA_["age_at_hire"]'}])),
                  log=log)

        custom_merges=[]
        for c in cons.custom_merges:
            if 'age_at_hire' in c:
                custom_merges.append(['age_at_hire_p1' if x=='age_at_hire' else x for x in c])
                custom_merges.append(['age_at_hire_m1' if x=='age_at_hire' else x for x in c])
            else:
                custom_merges.append(c)
            custom_merges.append({'query' : 'first_name_NS == "JEFFREY" & last_name_NS=="GOUGIS" & middle_initial=="L"',
                                  'cols' : ['first_name_NS', 'last_name_NS', 'middle_initial', "age_at_hire_m1"]})
            custom_merges.append({'query' : 'first_name_NS == "KATINA" & last_name_NS==["WATKINS", "WATKINSLLOYD"] & middle_initial=="S"',
                                  'cols' : ['first_name_NS', 'middle_initial', 'F4LN', "age_at_hire_p1"]})
            custom_merges.append({'query' : 'first_name_NS == "MICHAEL" & last_name_NS=="TEWS" & middle_initial=="J"',
                                  'cols' : ['first_name_NS', 'last_name_NS', 'middle_initial']})
        MD = (MD
            .loop_merge(base_OD=[], custom_merges=custom_merges, verbose=False)
            .append_to_reference(inplace=True))
        break

    if year == 2002:
        dfy = dfy.reuid(cons.ID)
        MD = MergeData(dfy, log=log)
    else:
        MD = (MD.add_sup_data(dfy)
                .loop_merge(base_OD=[], custom_merges=cons.custom_merges, verbose=False)
                .append_to_reference(inplace=True))

log.info('Number of unique IDs = %d', MD.rdf[cons.ID].nunique())

MD.to_FormatData\
    .write_data(cons.output_file)

sal = MD.rdf
sal[['start_date', 'spp_date', 'org_hire_date']] =\
    sal[['start_date', 'spp_date', 'org_hire_date']].apply(pd.to_datetime)
res_years = sal[[cons.ID, 'year', 'spp_date']]\
    .groupby(cons.ID, as_index=False)\
    .agg(max)\
    .rename(columns={'year' : 'resignation_year',
                     'spp_date' : 'max_spp_date'})
assert res_years.max_spp_date.isnull().sum() == \
    sal[cons.ID].nunique() - sal.loc[sal.spp_date.notnull(), cons.ID].nunique()
assert res_years.columns.tolist() == [cons.ID, 'resignation_year', 'max_spp_date']

sal = sal[[col for col in sal.columns
           if col in cons.profile_cols]].drop_duplicates()

assert sal[sal['start_date'].isnull() & sal['org_hire_date'].isnull()].empty

log.info('Creating so_max_date and so_min_date from max/min'
            ' of start_date and org_hire_date')

sal['so_max_date'] = sal[cons.ad_cols]\
                            .apply(pd.to_datetime)\
                            .max(axis=1).dt.date
sal['so_min_date'] = sal[cons.ad_cols]\
                            .apply(pd.to_datetime)\
                            .min(axis=1).dt.date

assert sal.so_min_date.isnull().sum() + sal.so_max_date.isnull().sum() == 0

sal.loc[sal['so_max_date'].isnull() &
        sal['start_date'].notnull(),
        'so_max_date'] = sal.loc[sal['so_max_date'].isnull() &
                                 sal['start_date'].notnull(),
                                 'start_date']
sal.loc[sal['so_min_date'].isnull() &
        sal['start_date'].notnull(),
        'so_min_date'] = sal.loc[sal['so_min_date'].isnull() &
                                 sal['start_date'].notnull(),
                                 'start_date']
sal.loc[sal['so_max_date'].isnull() &
        sal['org_hire_date'].notnull(),
        'so_max_date'] = sal.loc[sal['so_max_date'].isnull() &
                                 sal['org_hire_date'].notnull(),
                                 'org_hire_date']
sal.loc[sal['so_min_date'].isnull() &
        sal['org_hire_date'].notnull(),
        'so_min_date'] = sal.loc[sal['so_min_date'].isnull() &
                                 sal['org_hire_date'].notnull(),
                                 'org_hire_date']

log.info('Creating so_max_year and so_min_year from so_max_date and so_min_date')
sal['so_max_year'] = pd.to_datetime(sal['so_max_date']).dt.year
sal['so_min_year'] = pd.to_datetime(sal['so_min_date']).dt.year
sal['so_max_year_m1'] = sal['so_max_year']-1
sal['so_min_year_m1'] = sal['so_min_year']-1

log.info('Creating current_age columns from so_max_year and age_at_hire')
sal['current_age_p1'] = 2017 - (sal['so_min_year'] - sal['age_at_hire']) + 1
sal['current_age_m1'] = 2017 - (sal['so_min_year'] - sal['age_at_hire'])
sal['current_age_pm'] = sal['current_age_p1']
sal['current_age_mp'] = sal['current_age_m1']

sal['current_age2_p1'] = 2017 - (sal['so_max_year'] - sal['age_at_hire']) + 1
sal['current_age2_m1'] = 2017 - (sal['so_max_year'] - sal['age_at_hire'])
sal['current_age2_pm'] = sal['current_age2_p1']
sal['current_age2_mp'] = sal['current_age2_m1']

log.info('Adding resignation_year column = max year of observation')
sal = sal.merge(res_years, on=cons.ID, how='left')
log.info('Writing profiles data to %s' % cons.output_profiles_file)
sal.to_csv(cons.output_profiles_file, **cons.csv_opts)
