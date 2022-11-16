#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''merge script for roster__2018-03'''
import __main__
from setup import do_setup
from general_utils import FormatData
from merge_functions import MergeData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file': 'input/roster__2018-03.csv.gz',
        'output_file': 'output/roster__2018-03.csv.gz',
        'intrafile_id': 'roster__2018-03_ID',
        'input_reference_file' : 'input/officer-reference.csv.gz',
        'output_reference_file': 'output/officer-reference.csv.gz',
        'uid': 'NUID',
        'qfilter' : 'is_sworn_officer == "Y"',
        'radd_cols' : [
            'F4FN', 'F4LN', 'F2FN',
             {'exec' : '_DATA_["current_age_p1"] = 2018 - _DATA_["birth_year"]'},
             {'exec' : '_DATA_["current_age_m1"] = 2017 - _DATA_["birth_year"]'}],
        'sadd_cols' : [
            'F4FN', 'F4LN', 'F2FN',
             {'exec' : '_DATA_["current_age_p1"] = _DATA_["current_age"]'},
             {'exec' : '_DATA_["current_age_m1"] = _DATA_["current_age"]'},
             {'exec' : '_DATA_["star"]=_DATA_["current_star"]'}],
        'drop_cols' : ['F4FN', 'F4LN', 'F2FN', 'current_age_p1', 'current_age_m1', 'current_unit_detail', 'unit_id'],
        'fill_cols' : [
            'first_name_NS', 'last_name_NS', 'birth_year', 'appointed_date',
            'star', 'suffix_name', 'middle_initial', 'middle_initial2',
            'race', 'gender', 'current_unit', 'resignation_date'],
        'loop_merge' : {
            'base_OD_edits' :[
                ('birth_year', ['current_age_p1', 'current_age_m1', '']),
                ('first_name', ['first_name_NS', 'F4FN', 'F2FN'])],
            'custom_merges' : [
                ['first_name_NS', 'middle_initial', 'race', 'gender', 'current_age_p1', 'appointed_date', 'star'],
                ['first_name_NS', 'middle_initial', 'race', 'gender', 'current_age_m1', 'appointed_date', 'star'],
                {'query' : 'gender == "FEMALE"', 'cols' : ['first_name_NS', 'race', 'current_age_p1', 'appointed_date', 'star']},
                {'query' : 'gender == "FEMALE"', 'cols' : ['first_name_NS', 'race', 'current_age_m1', 'appointed_date', 'star']},
                {'query' : 'gender == "FEMALE"', 'cols' : ['first_name_NS', 'current_age_p1', 'appointed_date', 'star']},
                {'query' : 'gender == "FEMALE"', 'cols' : ['first_name_NS', 'current_age_m1', 'appointed_date', 'star']},
                {'query' : 'gender == "FEMALE"', 'cols' : ['first_name_NS', 'current_age_p1', 'appointed_date']},
                {'query' : 'gender == "FEMALE"', 'cols' : ['first_name_NS', 'current_age_m1', 'appointed_date']},
                ['last_name_NS', 'middle_initial', 'appointed_date', 'race', 'star', 'current_age_p1'],
                ['first_name_NS', 'last_name_NS', 'middle_initial', 'current_age_p1', 'race', 'gender']
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

rfd = \
FormatData(log=log)\
    .import_data(cons.input_reference_file, uid='NUID', add_row_id=False)
stored_rdf = rfd.data
rfd = rfd\
    .fill_data(fill_cols=cons.fill_cols, inplace=True)\
    .add_columns(cons.radd_cols)

sfd = \
FormatData(log=log)\
    .import_data(cons.input_file, uid=cons.intrafile_id)\
    .qfilter(cons.qfilter)\
    .add_columns(cons.sadd_cols)\
    .set_columns({'unit' : 'current_unit'})

MergeData(rfd, sfd, log=log)\
    .loop_merge(**cons.loop_merge)\
    .append_to_reference(inplace=True, full_rdf=stored_rdf,
                         drop_cols=cons.drop_cols)\
    .remerge_to_file(cons.input_file, cons.output_file)\
    .to_FormatData\
    .write_data(cons.output_reference_file)
