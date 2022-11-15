#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for complaints-accused_2000-2016_2016-11'''

import __main__
import pandas as pd
from setup import do_setup
from general_utils import FormatData
from import_functions import read_p046957_file

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_files': [
            'input/p046957_-_report_2.1_-_identified_accused.xls',
            'input/p046957_-_report_2.2_-_identified_accused.xls',
            'input/p046957_-_report_2.3_-_identified_accused.xls',
            'input/p046957_-_report_2.4_-_identified_accused.xls',
            'input/p046957_-_report_2.5_-_identified_accused.xls'
            ],
        'output_file': 'output/complaints-accused_2000-2016_2016-11.csv.gz',
        'column_names': [
            'cr_id', 'full_name', 'birth_year', 'gender', 'race',
            'appointed_date', 'current_unit', 'current_rank',
            'current_star', 'complaint_category',
            'recommended_finding', 'recommended_discipline',
            'final_finding', 'final_discipline'
            ],
        'ID': 'complaints-accused_2000-2016_2016-11_ID',
        'auid_args' : {
            'id_cols': [
                'first_name', 'last_name', 'suffix_name',
                'first_name_NS', 'last_name_NS',
                'appointed_date', 'birth_year', 'gender', 'race',
                ],
            'conflict_cols': [
                'middle_initial', 'middle_initial2',
                'current_unit', 'current_star', 'current_rank'
                ]
            }
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
    df, report_produced_date, FOIA_request = \
                                read_p046957_file(input_file,
                                                  original_crid_col='Number:')
    log.info(('Processing {0} file, of FOIA number {1}, produced on {2}'
              '').format(input_file, FOIA_request, report_produced_date))
    df.columns = cons.column_names
    log.info(('final_finding and recommended_finding '
              'column values of NA changed to NAF'))
    df.loc[df['final_finding'] == 'NA', 'final_finding'] = 'NAF'
    df.loc[df['recommended_finding'] == 'NA', 'recommended_finding'] = 'NAF'
    data_df = (data_df
               .append(df)
               .reset_index(drop=True))
log.info("Adding complaint type column (FPS, OPV, etc.) based on complaint_category")
ct_dict = {
    'UFV' : (
        "01A","01B","01C","03E","04H", "05A","05B","05C","05D","05E",
        "05F","05G","05H","05J","05K", "05L","05M","05N","05P","05Q","05T"
        ),
    'ALU' : ("04A","04B","04C","04D","04E","04F","04G","04J"),
    'Search' : ("03A","03B","03C","03D","03F","03G","03P"),
    'FPS' : ("10J", "10U"),
    'OPV' : (
        "07A","07B","07C","07D","07E","07F","07T",
        "10A","10B","10C","10D","10E","10F","10G","10H","10K", "10L","10M",
        "10N","10P","10Q","10R","10S","10T","10V", "10W","10X","10Y","10Z",
        "12A","12B","12C", "12D","12E","12F"
        )
    }
data_df['complaint_type'] = data_df['complaint_category'].map(
    lambda x:
        x if pd.isnull(x)
        else (
            [cat for cat in ct_dict if x[:3] in ct_dict[cat]][0]
            if any([cat for cat in ct_dict if x[:3] in ct_dict[cat]])
            else 'Other'
        )
)

FormatData(log=log)\
    .import_data(data_df)\
    .clean()\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .write_data(cons.output_file)
