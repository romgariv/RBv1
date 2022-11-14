#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for unit-history__2016-12'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/FOIA_P052262_-_11221-FOIA-P052262-AllSwornEmployeesWithUOA.xlsx',
        'output_file': 'output/unit-history__2016-12.csv.gz',
        'column_names_key': 'unit-history__2016-12',
        'ID': 'unit-history__2016-12_ID',
        'auid_args' : {
            'id_cols': [
                "first_name", "last_name", "suffix_name",
                "first_name_NS", "last_name_NS",
                "appointed_date", "birth_year", "gender",
                "Specify" # added for 1 individual
                ],
            'conflict_cols': ['middle_initial', 'middle_initial2'],
        }
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

FormatData(log=log)\
    .import_data(cons.input_file, sheets=1, column_names=cons.column_names_key)\
    .clean()\
    .add_columns([
        {'exec' : (
            '_DATA_["Specify"] = ((_DATA_["first_name"] == "ROBERT") & '
            '(_DATA_["last_name"] == "SMITH") & '
            '(_DATA_["middle_initial"] == "E") & '
            '(_DATA_["birth_year"] == 1947) & '
            '(_DATA_["unit"].isin([5, 602])))')}])\
    .LOG(("Robert E Smith 1947 1971-02-22 in units [5, 602]"
          " specified as singular individual."))\
    .assign_unique_ids(cons.ID, auid_args_dict=cons.auid_args)\
    .dcolumn('Specify')\
    .write_data(cons.output_file)
