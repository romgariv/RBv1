#! usr/bin/env python3
#
# Author:   Roman Rivera

'''pytest functions in assign_unique_ids_functions that require teardown/setup'''

import pytest
import pandas as pd
import numpy as np
import general_utils
import copy


def test_remove_duplicates():
    '''test remove_duplicates'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1],
         'B': [2, 2, 3, 4],
         'C': [1, 3, 1, 3]})
    orig_input_df = copy.deepcopy(input_df)
    output_df = pd.DataFrame(
        {'A': [1, 1],
         'B': [3, 4],
         'C': [1, 3]},
        index=[2, 3])

    results = general_utils.remove_duplicates(input_df,
                                              ['A', 'B'])
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


def test_keep_duplicates():
    '''test keep_duplicates'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1],
         'B': [2, 2, 3, 4],
         'C': [1, 3, 1, 3]})
    orig_input_df = copy.deepcopy(input_df)
    output_df = pd.DataFrame(
        {'A': [1, 1],
         'B': [2, 2],
         'C': [1, 3]},
        index=[0, 1])

    results = general_utils.keep_duplicates(input_df,
                                            ['A', 'B'])
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)

def test_keep_conflicts_all():
    '''test keep_conflicts with all=True'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 2, 10, 11, 12],
         'B': [2, 2, 3, 4, 3, 10, 3, 12],
         'C': [1, 3, 1, 3, 3, 10, 11, 1]})
    input_args = {'cols' : ['A','B'], 'all_dups' : True}
    orig_input_df = copy.deepcopy(input_df)
    output_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 2, 11],
         'B': [2, 2, 3, 4, 3, 3],
         'C': [1, 3, 1, 3, 3, 11]},
        index=[0, 1, 2, 3, 4, 6],
        columns=['A','B','C'])

    results = general_utils.keep_conflicts(input_df, **input_args)

    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)

def test_keep_conflicts_not_all():
    '''test keep_conflicts with all=False'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 1, 1, 2, 10, 11],
         'B': [2, 2, 3, 4, 3, 10, 3],
         'C': [1, 3, 1, 3, 3, 10, 11]})
    input_args = {'cols' : ['A', 'B'], 'all_dups' : False}
    orig_input_df = copy.deepcopy(input_df)
    output_df = pd.DataFrame(
        {'A': [1, 1, 1],
         'B': [2, 2, 3],
         'C': [1, 3, 1]},
        index=[0, 1, 2],
        columns=['A','B','C'])

    results = general_utils.keep_conflicts(input_df, **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


def test_reshape_data():
    '''test reshape_data'''
    input_df = pd.DataFrame(
        {'A': [1, 2, 3,4],
         'B1': [1,2, np.nan, 5,],
         'B2': [1,np.nan,np.nan, 6],
         'C' : [10,11,12, 13]})
    input_args = {'reshape_col' : 'B', 'id_col' : 'A'}
    orig_input_df = copy.deepcopy(input_df)
    output_df = pd.DataFrame(
        {'A': [1, 2, 4, 4, 3],
         'B': [1, 2, 5,6, np.nan],
         'C': [10, 11, 13, 13, 12]},
        columns=['A','B','C'])

    results = general_utils.reshape_data(input_df, **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


def test_fill_data():
    '''test fill_data'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', 'BOB', 'AN', 'ANNIE', 'ANNA'],
         'B': ['M', np.nan, np.nan, 'L', 'L'],
         'C': ['JR', np.nan, np.nan, np.nan, np.nan],
         'D': ['JONES', 'JONES' , 'SMITH', 'SMITH', 'ORIELY'],
         'E': [10, 50, 20, 2, np.nan]})
    orig_input_df = copy.deepcopy(input_df)
    input_args = {'id_col' : 'id'}
    output_df = pd.DataFrame(
        {'id' : [1,1, 2, 2, 2, 2, 3],
         'A': ['BOB','BOB', 'AN', 'AN', 'ANNA','ANNA', 'ANNIE'],
         'B': ['M','M', 'L', 'L', 'L', 'L', 'L'],
         'C': ['JR','JR', np.nan, np.nan, np.nan, np.nan, np.nan],
         'D': ['JONES','JONES', 'SMITH', 'ORIELY', 'SMITH', 'ORIELY', 'SMITH'],
         'E' : [10.0, 50.0, 20.0, 20.0, 20.0, 20.0, 2.0]})
    results = general_utils.fill_data(input_df, **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


# def test_union_group():
#     '''test union_group'''
#     input_df = pd.DataFrame([
#         (np.nan, np.nan, np.nan),
#         (1,1,1),
#         (1,2,1),
#         (np.nan,4,1),
#         (4,4,np.nan),
#         (np.nan, np.nan, 10),
#         (0,0,10),
#         (9,10,np.nan)],
#         columns=['A','B','C'],
#         index=[2,100,11,12,13, 15, 66, 7])
#     orig_input_df = copy.deepcopy(input_df)
#     input_args = {'gid' : 'gid', 'cols' : ['A', 'B', 'C']}

#     output_df = pd.DataFrame([
#         (1,1,1,1),
#         (1,2,1,1),
#         (np.nan,4,1,1),
#         (4,4,np.nan,1),
#         (np.nan, np.nan, 10,2),
#         (0,0,10,2),
#         (9,10,np.nan,3),
#         (np.nan, np.nan, np.nan,4)],
#         columns = ['A', 'B', 'C','gid'],
#         index=[100,11,12,13, 15, 66, 7, 2])

#     results = general_utils.union_group(input_df, **input_args)
#     assert orig_input_df.equals(input_df)
#     assert results.equals(output_df)

def test_collapse_data():
    '''test collapse_data'''
    input_df = pd.DataFrame(
        {'A' : [1, 2, 1, 3, 2, 1],
         'B': ['BOB', 'AN', 'BOB', np.nan, 'AN', 'BOB']},
         index=[1,20,10,500,201,101],
         columns = ['A', 'B'])
    orig_input_df = copy.deepcopy(input_df)
    output_collapsed_df = pd.DataFrame({
        'A' : [1, 2, 3],
        'B' : ['BOB', 'AN', np.nan]},
        columns = ['A', 'B'])
    output_stored_df = pd.DataFrame(
        {'Index' : [1,10,101,20,201,500],
         'TempID' : [0, 0, 0, 1, 1, 2]},
         columns = ['Index', 'TempID'])

    results = general_utils.collapse_data(input_df)
    assert results[0].equals(output_collapsed_df)
    assert results[1].equals(output_stored_df)
    assert orig_input_df.equals(input_df)

def test_expand_data():
    '''test expand_data'''
    input_collapsed_df = pd.DataFrame({
        'A' : [1, 2, 3],
        'B' : ['BOB', 'AN', np.nan]},
        columns = ['A', 'B'])
    input_stored_df = pd.DataFrame({
        'Index' : [1,10,20,101,201,500],
        'TempID' : [0, 0, 0, 1, 1, 2]},
        columns = ['Index', 'TempID'])
    orig_input_collapsed_df = copy.deepcopy(input_collapsed_df)
    orig_input_stored_df = copy.deepcopy(input_stored_df)
    output_full_df = pd.DataFrame(
        {'A' : [1, 1, 1, 2, 2, 3],
         'B': ['BOB', 'BOB', 'BOB', 'AN', 'AN', np.nan]},
         index=[1,10,20,101,201,500],
         columns = ['A', 'B'])
    results = general_utils.expand_data(input_collapsed_df, input_stored_df)
    assert results.equals(output_full_df)
    assert orig_input_collapsed_df.equals(input_collapsed_df)
    assert orig_input_stored_df.equals(input_stored_df)

########## FormatData Method Tests ##########
import assign_unique_ids_functions

def test_FormatData1():
    '''test initializing ref'''
    input_df = pd.DataFrame(
        {'data_id' : [10, 20, 30, 1, 109, 110],
         'first_name_NS': ['BOB', 'BOB', 'KATHLEEN', 'KEVIN', 'ELLEN', 'DEAN'],
         'middle_initial': ['M', np.nan, np.nan, 'J', 'L', 'E'],
         'suffix_name': ['SR', 'JR', np.nan, 'II', np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'SMITH', 'PARK', 'ORIELY', 'SMITH'],
         'star1': [10, 50, 20, 2, np.nan, 2],
         'star2': [20, 51, np.nan, 4, np.nan, 3],
         'star3': [30, np.nan, np.nan, 10, np.nan, np.nan],
         'merge': [1, 1, 1, 0, 1, 1]})
    input_args = {'uid': 'data_id',
                  'log': None}
    auid_args = {'new_uid' : 'UID', 'start_uid' : 5, 'auid_args_dict' : {'id_cols' : ['data_id']}}

    FD = general_utils.FormatData(input_df, **input_args)\
        .qfilter('merge == 1')\
        .dcolumn('merge')\
        .assign_unique_ids(**auid_args)\
        .reshape_long('star')\
        .add_columns([
                "F4FN",
                {'exec' : "_DATA_['L4LN'] = _DATA_['last_name_NS'].map(lambda x: x[-4:None])"}])

    output_df = pd.DataFrame(
        {'data_id' : [10, 10, 10, 20, 20, 30, 109, 110, 110],
         'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATHLEEN', 'ELLEN', 'DEAN', 'DEAN'],
         'middle_initial': ['M', 'M', 'M', np.nan, np.nan, np.nan, 'L', 'E', 'E'],
         'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY', 'SMITH', 'SMITH'],
         'star' : [10, 20, 30, 50, 51, 20, np.nan, 2, 3],
         'F4FN' : ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATH', 'ELLE', 'DEAN', 'DEAN'],
         'L4LN' : ['ONES', 'ONES', 'ONES', 'ONES', 'ONES', 'MITH', 'IELY', 'MITH', 'MITH'],
         'UID' : [5, 5, 5, 6, 6, 7, 8, 9, 9]})

    results = FD.data
    assert set(results.columns) == set(output_df.columns)
    assert results.equals(output_df[results.columns])
    assert FD.uid == 'UID'


def test_FormatData2():
    '''test initializing sup'''
    input_sup_df = pd.DataFrame(
        {'sid' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'birth_year' : [1970, 1990, 1985, 1965, 1986],
         'middle_initial': [np.nan, 'M', 'C', 'L', 'E'],
         'suffix_name': ['SR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'current_star': [np.nan, 51, 20, 100, 192]})
    input_sup_args = {'uid': 'sid',
                      'log': None}

    first_four = lambda x: x[:4]
    FD2 = general_utils.FormatData(input_sup_df, **input_sup_args)\
            .set_columns({'current_star' : 'star', 'sid' : 'SID'})\
            .add_columns([
                "F4FN",
                {'exec' : "_DATA_['L4LN'] = _DATA_['last_name_NS'].map(lambda x: x[-4:None])"},
                {'out_col' : 'F4LN', 'in_col' : 'last_name_NS', 'func' : first_four}])
    output_df = pd.DataFrame(
        {'SID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'birth_year' : [1970, 1990, 1985, 1965, 1986],
         'middle_initial': [np.nan, 'M', 'C', 'L', 'E'],
         'suffix_name': ['SR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'star': [np.nan, 51, 20, 100, 192],
         'F4FN': ['BOB', 'BOB', 'KATH', 'ELLE', 'JENN'],
         'L4LN': ['ONES', 'ONES', 'RANT', 'IELY', 'ONES'],
         'F4LN': ['JONE', 'JONE' , 'GRAN', 'SKAR', 'JONE']})
    results = FD2.data
    assert set(results.columns) == set(output_df.columns)
    assert results.equals(output_df[results.columns])
    assert FD2.uid == 'SID'


def test_FormatData3():
    '''test FormatData'''
    input_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3],
         'first_name_NS': ['BOB',  'AN', 'AN'],
         'middle_initial': [np.nan, 'L', 'L'],
         'suffix_name': [np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'ORIELY', 'SMITH'],
         'current_star': [50, 20, np.nan],
         'star': [50, 20, np.nan]})
    SD = general_utils.FormatData(input_sup_df, uid='sub__2016_ID')\
        .add_columns(["F2FN"])\
        .fill_data(fill_cols=['first_name_NS', 'middle_initial',
                      'suffix_name', 'last_name_NS', 'star'])
    results = SD.data
    output_df = pd.DataFrame(
        {'sub__2016_ID' : pd.Series([1, 2, 3], dtype='O'),
         'first_name_NS': ['BOB',  'AN', 'AN'],
         'F2FN': ['BO',  'AN', 'AN'],
         'middle_initial': [np.nan, 'L', 'L'],
         'suffix_name': [np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'ORIELY', 'SMITH'],
         'current_star': [50, 20, np.nan],
         'star': [50, 20, np.nan]})
    assert results.equals(output_df[results.columns])


def test_FormatData_add_columns():
    '''test FormatData.add_columns'''
    input_df = pd.DataFrame(
        {'data_id' : [10, 20, 30, 1, 109, 110],
         'first_name_NS': ['BOB', 'BOB', 'KATHLEEN', 'KEVIN', 'ELLEN', 'DEAN'],
         'star1': [10, 50, 20, 2, np.nan, 2],
         'star2': [30, np.nan, np.nan, 10, np.nan, np.nan]})

    FD = general_utils.FormatData(input_df, uid='data_id')\
        .add_columns([
                "F4FN",
                {'exec' : "_DATA_['star'] = _DATA_[['star1', 'star2']].apply(max,1)"},
                {'exec' : "_DATA_['A'] = list(range(_DATA_.shape[0]))"}])

    output_df = pd.DataFrame(
        {'data_id' : [10, 20, 30, 1, 109, 110],
         'first_name_NS': ['BOB', 'BOB', 'KATHLEEN', 'KEVIN', 'ELLEN', 'DEAN'],
         'star1': [10, 50, 20, 2, np.nan, 2],
         'star2': [30, np.nan, np.nan, 10, np.nan, np.nan],
         'F4FN': ['BOB', 'BOB', 'KATH', 'KEVI', 'ELLE', 'DEAN'],
         'star': [30, 50, 20, 10, np.nan, 2],
         'A': [0,1,2,3,4,5]})

    results = FD.data
    assert set(results.columns) == set(output_df.columns)
    assert results.equals(output_df[results.columns])


def test_FormatData_aggregate():
    '''test FormatData.aggregate'''
    # Copied from test_assign_unique_ids_functions.test_aggregate_data

    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'C', 'C'],
         'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'max': [1, np.nan, 10, 1, 3, 9, 2, 2, -2, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})
    orig_input_df = copy.deepcopy(input_df)

    input_args = {'id_cols': ['ID'],
                  'mode_cols': ['mode'],
                  'max_cols': ['max'],
                  'current_cols': ['age'],
                  'time_col': 'date_of_age_obs',
                  'merge_cols': ['max_names'],
                  'merge_on_cols': ['max']}

    results = general_utils.FormatData(input_df, uid='uid')\
        .aggregate(input_args)\
        .data

    output_df = pd.DataFrame(
        {'uid': [1, 4, 99],
         'ID': ['A', 'B', 'C'],
         'mode': [2.0, 1.0, 5.0],
         'max': [10, 2, np.nan],
         'current_age': [23, 57, np.nan],
         'max_names': ['Ten', 'Two', np.nan]},
        columns=['uid', 'ID', 'mode', 'max', 'current_age', 'max_names'])

    input_args['uid'] = 'uid'
    results = assign_unique_ids_functions.aggregate_data(input_df,
                                                         **input_args)
    assert results.equals(output_df)
    assert orig_input_df.equals(input_df)


def test_FormatData_aggregate_inplaceFalse():
    '''test FormatData.aggregate with inplace=False'''
    # Copied from test_assign_unique_ids_functions.test_aggregate_data

    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'C', 'C'],
         'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'max': [1, np.nan, 10, 1, 3, 9, 2, 2, -2, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})
    orig_input_df = copy.deepcopy(input_df)

    input_args = {'id_cols': ['ID'],
                  'mode_cols': ['mode'],
                  'max_cols': ['max'],
                  'current_cols': ['age'],
                  'time_col': 'date_of_age_obs',
                  'merge_cols': ['max_names'],
                  'merge_on_cols': ['max']}

    FD = general_utils.FormatData(input_df, uid='uid')

    results = FD.aggregate(input_args, inplace=False)

    output_df = pd.DataFrame(
        {'uid': [1, 4, 99],
         'ID': ['A', 'B', 'C'],
         'mode': [2.0, 1.0, 5.0],
         'max': [10, 2, np.nan],
         'current_age': [23, 57, np.nan],
         'max_names': ['Ten', 'Two', np.nan]},
        columns=['uid', 'ID', 'mode', 'max', 'current_age', 'max_names'])

    input_args['uid'] = 'uid'
    results = assign_unique_ids_functions.aggregate_data(input_df,
                                                         **input_args)
    assert results.equals(output_df)
    assert FD.data.equals(orig_input_df)
    assert orig_input_df.equals(input_df)


def test_FormatData_clean():
    '''test FormatData.clean'''
    # Taken from test_clean_functions.test_clean()
    input_df = pd.DataFrame(
       {'first_name' : ['j edgar','Dylan ., JR', 'Mary sue E. M.', 'nAtAsha',''],
        'last_name' : ['Hoover., iii','Smith F', 'Jones V', "O.'brien-jenkins IV", np.nan],
        'middle_initial' : ['A', 'B', np.nan, 'C', 'D'],
        'incident_datetime' : ['9999-99-99 12:12', '2016-01-21', '2015-12-52 100:100', '2016-01-12 02:54', '07/21/16 10:59'],
        'trr_date' : ['200-12-12', '2000-12-12', '1921-01-01', '2016-12-01', '07/21/21'],
        'trr_time' : [1212, "00", 9876, "23:12", 109],
        'age' : [120, -999, 0, 21, "hi"],
        'race' : ['N', 'wbh', 'naTIVE AMericaN', 'black hispanic',  'I'],
        'gender' : ['mALE', 'm', 'NONE', 'FEMALE', np.nan],
        'age' : [1,2,3,'a', 'b']
        })

    output_df = pd.DataFrame(
       {'first_name' : ['J EDGAR','DYLAN', 'MARY SUE', 'NATASHA',np.nan],
        'last_name' : ['HOOVER','SMITH', 'JONES', "O'BRIEN-JENKINS", np.nan],
        'first_name_NS' : ['JEDGAR','DYLAN', 'MARYSUE', 'NATASHA',np.nan],
        'last_name_NS' : ['HOOVER','SMITH', 'JONES', 'OBRIENJENKINS', np.nan],
        'middle_initial' : ['A', 'B', 'E', 'C', 'D'],
        'middle_initial2' : [np.nan, 'F', 'M', np.nan, np.nan],
        'suffix_name' : ['III', 'JR', 'V', 'IV', np.nan],
        'incident_date' : pd.to_datetime(pd.Series([np.nan, '2016-01-21', np.nan, '2016-01-12', '2016-07-21'])).dt.date,
        'incident_time' : pd.to_datetime(pd.Series(['12:12:00', '00:00:00', np.nan, '02:54:00', '10:59:00'])).dt.time,
        'trr_date' : pd.to_datetime(pd.Series([np.nan, '2000-12-12', '1921-01-01', '2016-12-01', '1921-07-21'])).dt.date,
        'trr_time' : pd.to_datetime(pd.Series(['12:12:00', '00:00:00', np.nan, '23:12:00', '01:09:00'])).dt.time,
        'age' : [np.nan, np.nan, np.nan, 21, np.nan],
        'race' : [1, 2, 0, 1, 0],
        'gender' : ['MALE', 'MALE', np.nan, 'FEMALE', np.nan],
        'age' : [1,2,3,'a', 'b']
        })

    results = general_utils.FormatData(input_df).clean({'skip_cols' : ['age'], 'clean_dict' : {'race' : {'N' : 1, 'wbh' : 2, 'naTIVE AMericaN' : 0, 'black hispanic' : 1, 'I' : 0}}}).data
    assert set(results.columns) == set(output_df.columns)
    assert results.equals(output_df[results.columns])


def test_FormatData_columns():
    '''test FormatData.columns'''

    input_df = pd.DataFrame(
        {'uid': [],
         'ID': [],
         'mode': [],
         'date_of_age_obs': [],
         'age': [],
         'max': [],
         'max_names': []})
    orig_input_df = copy.deepcopy(input_df)

    assert all(general_utils.FormatData(input_df, uid='uid').columns == orig_input_df.columns)


def test_FormatData_copy_col():
    '''test FormatData.copy_col'''

    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'C', 'C'],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})
    orig_input_df = copy.deepcopy(input_df)
    FD = general_utils.FormatData(input_df, uid='uid')\
        .copy_col('ID', 'ID_copy')\
        .copy_col('date_of_age_obs', 'date_of_age_obs_copy')\
        .copy_col('max_names', 'max_names_copy')

    assert set(FD.columns.tolist()) == set(input_df.columns.tolist() + ['ID_copy', 'date_of_age_obs_copy', 'max_names_copy'])
    assert FD.data['ID_copy'].equals(FD.data['ID'])
    assert FD.data['max_names_copy'].equals(FD.data['max_names'])
    assert FD.data['date_of_age_obs_copy'].equals(FD.data['date_of_age_obs'])


def test_FormatData_data():
    '''test FormatData.data'''

    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'C', 'C'],
         'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'max': [1, np.nan, 10, 1, 3, 9, 2, 2, -2, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})
    orig_input_df = copy.deepcopy(input_df)

    FD = general_utils.FormatData(input_df, uid='uid')
    assert FD.data.equals(orig_input_df)


def test_FormatData_date_to_month_to_datetimeTrue():
    '''test FormatData.date_to_month with to_datetime=True'''
    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'date_of_age_obs': ['2014-01-12', '2014-03-43', '2015-01-11',
                             2123, 12, '2015-11-04',
                             '2015-01-3', '2016-01-31', 'hi',
                             '20-21-123', np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30]})
    orig_input_df = copy.deepcopy(input_df)

    FD = general_utils.FormatData(input_df)\
        .date_to_month('date_of_age_obs', 'month')
    output_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'date_of_age_obs': ['2014-01-12', '2014-03-43', '2015-01-11',
                             2123, 12, '2015-11-04',
                             '2015-01-3', '2016-01-31', 'hi',
                             '20-21-123', np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'month': pd.to_datetime(pd.Series(
            ['2014-01-01', pd.NaT, '2015-01-01', '2123-01-01',
            pd.NaT, '2015-11-01', '2015-01-01',
            '2016-01-01', pd.NaT, pd.NaT, pd.NaT]))})
    assert FD.data.equals(output_df)


def test_FormatData_date_to_month_to_datetimeFalse():
    '''test FormatData.date_to_month with to_datetime=False'''
    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'date_of_age_obs': ['2014-01-12', '2014-03-43', '2015-01-11',
                             2123, 12, '2015-11-04',
                             '2015-01-3', '2016-01-31', 'hi',
                             '20-21-123', np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30]})
    orig_input_df = copy.deepcopy(input_df)

    FD = general_utils.FormatData(input_df)\
        .date_to_month('date_of_age_obs', 'month', to_datetime=False)
    output_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'date_of_age_obs': ['2014-01-12', '2014-03-43', '2015-01-11',
                             2123, 12, '2015-11-04',
                             '2015-01-3', '2016-01-31', 'hi',
                             '20-21-123', np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'month': pd.Series(
            ['2014-01-01', pd.NaT, '2015-01-01', '2123-01-01',
            pd.NaT, '2015-11-01', '2015-01-01',
            '2016-01-01', pd.NaT, pd.NaT, pd.NaT])})
    assert FD.data.equals(output_df)


def test_FormatData_dcolumn():
    '''test FormatData.dcolumn'''
    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'C', 'C'],
         'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'max': [1, np.nan, 10, 1, 3, 9, 2, 2, -2, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})
    FD = general_utils.FormatData(input_df, uid='uid')\
        .dcolumn(['uid', 'ID'])\
        .dcolumn('age')

    output_df = pd.DataFrame(
        {'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'max': [1, np.nan, 10, 1, 3, 9, 2, 2, -2, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})
    assert FD.data.equals(output_df)


def test_FormatData_dropna():
    '''test FormatData.dropna'''
    # Wraps pandas.DataFrame.dropna()
    assert True


def test_FormatData_fill_data():
    '''test FormatData.fill_data'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', 'BOB', 'AN', 'ANNIE', 'ANNA'],
         'B': ['M', np.nan, np.nan, 'L', 'L'],
         'C': ['JR', np.nan, np.nan, np.nan, np.nan],
         'D': ['JONES', 'JONES' , 'SMITH', 'SMITH', 'ORIELY'],
         'E': [10, 50, 20, 2, np.nan],
         'F' : [1,2,3,4,5]})
    orig_input_df = copy.deepcopy(input_df)
    output_df = pd.DataFrame(
        {'id' : [1,1, 2, 2, 2, 2, 3],
         'A': ['BOB','BOB', 'AN', 'AN', 'ANNA','ANNA', 'ANNIE'],
         'B': ['M','M', 'L', 'L', 'L', 'L', 'L'],
         'C': ['JR','JR', np.nan, np.nan, np.nan, np.nan, np.nan],
         'D': ['JONES','JONES', 'SMITH', 'ORIELY', 'SMITH', 'ORIELY', 'SMITH'],
         'E' : [10.0, 50.0, 20.0, 20.0, 20.0, 20.0, 2.0]})
    FD = general_utils.FormatData(input_df, uid='id')\
        .fill_data(['A', 'B', 'C', 'D', 'E'])

    assert FD.data.equals(output_df)
    assert orig_input_df.equals(input_df)

def test_FormatData_fillna_default():
    '''test FormatData.fillna default(val=0)'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', np.nan, 'B', 'ANNIE', 'ANNA'],
         'B': [-1, np.nan, np.nan, 0, 10]})
    output_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', np.nan, 'B', 'ANNIE', 'ANNA'],
         'B': [-1.0, 0.0, 0.0, 0.0, 10.0]})
    FD = general_utils.FormatData(input_df, uid='id')\
        .fillna('B')
    assert FD.data.equals(output_df)


def test_FormatData_fillna_valhi():
    '''test FormatData.fillna val="hi"'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', np.nan, 'B', 'ANNIE', 'ANNA'],
         'B': [-1, np.nan, np.nan, 0, 10]})
    output_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', 'hi', 'B', 'ANNIE', 'ANNA'],
         'B': [-1.0, 'hi', 'hi', 0.0, 10.0]})
    FD = general_utils.FormatData(input_df, uid='id')\
        .fillna(['B','A'], val='hi')
    assert FD.data.equals(output_df)


def test_FormatData_generate_dummy_cols():
    '''test FormatData.generate_dummy_cols'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', np.nan, 'B', 'ANNIE', 'ANNA'],
         'B': ['b', 'w', 'h', 'h', 'b']})
    output_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', np.nan, 'B', 'ANNIE', 'ANNA'],
         'B': ['b', 'w', 'h', 'h', 'b'],
         'DC_b' : [1,0,0,0,1],
         'DC_w' : [0,1,0,0,0],
         'DC_h' : [0,0,1,1,0]})
    FD = general_utils.FormatData(input_df, uid='id')\
        .generate_dummy_cols('B', 'DC_')
    assert FD.data.equals(output_df)


def test_FormatData_import_data1():
    '''test FormatData.import_data'''

    results = general_utils.FormatData()\
        .import_data('import_data_files/test1.csv',
                     add_row_id=True, skiprows=2,
                     column_names=['x','y','z'])\
        .data
    output_df = pd.DataFrame({
        'ROWID': [1,2],
        'x': [1,4],
        'y': [2, np.nan],
        'z': [3, 5]})

    assert results.equals(output_df)

def test_FormatData_import_data2():
    '''test FormatData.import_data'''

    results = general_utils.FormatData()\
        .import_data('import_data_files/test2.csv',
                     skiprows=1, strip_colnames=True,
                     add_row_id=False,
                     column_names={'a' : 'c', 'c' : 'a'})\
        .data
    output_df = pd.DataFrame({
        'c': [1,4],
        'b': [2, np.nan],
        'a': [3,5]})
    assert results.equals(output_df)


def test_FormatData_import_data3():
    '''test FormatData.import_data'''

    results = general_utils.FormatData()\
        .import_data(['import_data_files/test3_1.csv',
                      'import_data_files/test3_2.csv',
                      'import_data_files/test3_3.csv'],
                     skiprows=1,
                     column_names={'a' : 'c', 'c' : 'a'},
                     concat_axis=0, dall_nan=True)\
        .data
    output_df = pd.DataFrame({
        'ROWID': [1,2,3,4],
        'c': [1,4,6, np.nan],
        'b': [2, np.nan, np.nan,9],
        'a': [3,5,7,10.0],
        'FILE': ['import_data_files/test3_1.csv',
                 'import_data_files/test3_1.csv',
                 'import_data_files/test3_2.csv',
                 'import_data_files/test3_3.csv']})
    assert results.equals(output_df)


def test_FormatData_import_data4():
    '''test FormatData.import_data'''

    results = general_utils.FormatData()\
        .import_data('import_data_files/test4.xls',
                     no_header=True,
                     sheets=['s1', 's2', 's4'],
                     concat_axis=1)\
        .data
    output_df = pd.concat([
        pd.DataFrame({
            'ROWID': [1,2],
            0 : [1,4],
            1 : [2,5],
            2 : [3,6],
            'SHEET' : ['s1','s1']}),
        pd.DataFrame({
            0 : [8,np.nan],
            1 : [9,1],
            2 : [0,0],
            'SHEET' : ['s2','s2']}),
        pd.DataFrame({
            0 : [np.nan,3],
            1 : [1,4],
            2 : [2,np.nan],
            'SHEET' : ['s4','s4']})], axis=1)
    assert results.equals(output_df)


def test_FormatData_import_data5():
    '''test FormatData.import_data'''
    # DO, maybe check column names files?
    assert True


def test_FormatData_kcolumn():
    '''test FormatData.kcolumn'''
    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'C', 'C'],
         'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'max': [1, np.nan, 10, 1, 3, 9, 2, 2, -2, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})
    FD = general_utils.FormatData(input_df, uid='uid')\
        .kcolumn(['mode'])
    output_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5]})
    assert FD.data.equals(output_df)


def test_FormatData_lookup():
    '''test FormatData.date_to_month'''
    input_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 4, 4, 4, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'C', 'C'],
         'mode': [2, 2, 2, 3, 3, 3, 1, np.nan, 4, 5, 5],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             '2015-01-01', '2016-01-01', np.nan,
                             np.nan, np.nan],
         'age': [20, 20, 21, 22, 23, 22, 56, 57, 90, 35, 30],
         'max': [1, np.nan, 10, 1, 3, 9, 2, 2, -2, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                       'Two', 'Two', '-Two', np.nan, np.nan]})
    results = general_utils.FormatData(input_df, uid='uid')\
        .lookup([1,99])
    output_df = pd.DataFrame(
        {'uid': [1, 1, 1, 1, 1, 1, 99, 99],
         'ID': ['A', 'A', 'A', 'A', 'A', 'A', 'C', 'C'],
         'mode': [2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 5.0, 5.0],
         'date_of_age_obs': ['2014-01-01', '2014-03-01', '2015-01-01',
                             '2015-09-01', '2016-10-01', '2015-11-01',
                             np.nan, np.nan],
         'age': [20, 20, 21, 22, 23, 22, 35, 30],
         'max': [1, np.nan, 10, 1, 3, 9, np.nan, np.nan],
         'max_names': ['One', np.nan, 'Ten', 'One', 'Three', 'Nine',
                        np.nan, np.nan]},
        index=[0,1,2,3,4,5,9,10])

    assert results.equals(output_df)


def test_FormatData_map_func():
    '''test FormatData.map with function'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', 2, 'B', 'ANNIE', 'ANNA']})
    output_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOBx', '2x', 'Bx', 'ANNIEx', 'ANNAx']})
    FD = general_utils.FormatData(input_df, uid='id')\
        .map('A', lambda x: str(x) + 'x')
    assert FD.data.equals(output_df)


def test_FormatData_map_constant():
    '''test FormatData.map with constant'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', np.nan, 'B', 'ANNIE', 'ANNA']})
    output_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['BOB', np.nan, 'B', 'ANNIE', 'ANNA'],
         'B': [np.nan, np.nan, np.nan, np.nan, np.nan]})
    FD = general_utils.FormatData(input_df, uid='id')\
        .map('B',np.nan)
    assert FD.data.equals(output_df)


def test_FormatData_qfilter():
    '''test FormatData.qfilter'''
    input_df = pd.DataFrame(
        {'id' : [1, np.nan, 2, 3, 2],
         'A': ['BOB', "B", 'B', 'ANNIE', 'ANNA']})
    output_df = pd.DataFrame(
        {'id' : [1.0],
         'A': ['BOB']})
    FD = general_utils.FormatData(input_df)\
        .qfilter('A == ["BOB", "B"]')\
        .qfilter('id==id')\
        .qfilter('id <= 1')
    assert FD.data.equals(output_df)


def test_FormatData_recode():
    '''test FormatData.recode'''
    input_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['x', 'x', 'z', 'y', 0],
         'B': [3, 2, 4, 5, 3],
         'C': [3, 2, 4, 5, 3]})
    FD = general_utils.FormatData(input_df)\
        .recode('A', 'A2', {'x' : 1, 'y' : np.nan, 'z' : '1'}, missing_na=False)\
        .recode('B', 'B', {1 : [2,3], np.nan : 5}, missing_na=True)\
        .recode('C', 'C2', {1 : [2,3]}, missing_na=False)\
        .recode('C', 'C', {1 : [2,3], 6 : 5}, missing_na=True)

    output_df = pd.DataFrame(
        {'id' : [1, 1, 2, 3, 2],
         'A': ['x', 'x', 'z', 'y', 0],
         'B': [1, 1, np.nan, np.nan, 1],
         'C': [1, 1, np.nan, 6, 1],
         'A2': [1.0, 1.0, '1', np.nan, 0.0],
         'C2': [1.0, 1.0, 4.0, 5.0, 1.0],})
    assert FD.data.equals(output_df)

def test_FormatData_reshape_long():
    '''test FormatData.reshape_long'''
    input_df = pd.DataFrame(
        {'A': [1, 2, 3,4],
         'B1': [1,2, np.nan, 5,],
         'B2': [1,np.nan,np.nan, 6],
         'C' : [10,11,12, 13]})
    FD = general_utils.FormatData(input_df, uid='A')\
        .reshape_long('B')
    output_df = pd.DataFrame(
        {'A': [1, 2, 3, 4, 4],
         'B': [1, 2,np.nan, 5,6],
         'C': [10, 11, 12, 13, 13]},
        columns=['A','B','C'])

    assert FD.data.equals(output_df)


def test_FormatData_reuid():
    '''test FormatData.reuid'''
    input_df = pd.DataFrame(
        {'A': [100, 200, 300,400],
         'B' : [10,11,9, 13]})
    FD = general_utils.FormatData(input_df, uid='A')\
        .reuid('ID', skip_query='B >= 10')
    output_df = pd.DataFrame(
        {'A': [100, 200, 300,400],
         'B' : [10,11,9, 13],
         'ID' : [1, 2, np.nan, 3]})
    assert FD.data.equals(output_df)
    assert FD.uid == 'ID'


def test_FormatData_set_columns():
    '''test FormatData.set_columns'''
    input_df = pd.DataFrame(
        {'A': [1,2],
         'B': [1,2],
         'C': [1,2]})
    FD = general_utils.FormatData(input_df, uid='A')\
        .set_columns(['x', 'y', 'z'])\
        .set_columns({'x' : 1, 'y' : 'B', 'k' : 'E'})

    output_df = pd.DataFrame(
        {1: [1,2],
         'B': [1,2],
         'z': [1,2]})
    assert FD.data.equals(output_df)
    assert FD.uid == 1


def test_FormatData_sort():
    '''test FormatData.sort'''
    input_df = pd.DataFrame(
        {'A': [1,2,2],
         'B': [1,np.nan,2]})

    output_df1 = pd.DataFrame(
        {'A': [2,2,1],
         'B': [np.nan, 2, 1]})
    FD = general_utils.FormatData(input_df, uid='A')\
        .sort(reverse=True)
    assert FD.data.equals(output_df1)

    output_df2 = pd.DataFrame(
        {'A': [1,2,2],
         'B': [1, 2, np.nan]})
    FD = general_utils.FormatData(input_df, uid='A')\
        .sort(cols=['B'])
    assert FD.data.equals(output_df2)


def test_FormatData_uid():
    '''test FormatData.uid'''
    input_df = pd.DataFrame(
        {'A': [1,2,2],
         'B': [1,np.nan,2]})
    FD = general_utils.FormatData(input_df, uid='B')
    assert FD.uid == 'B'


def test_FormatData_unique():
    '''test FormatData.unique'''
    input_df = pd.DataFrame(
        {'A': [1, 1, 3,3],
         'B': [1,2, 5, 5,],
         'C' : [np.nan,np.nan,12, 13]})

    FD = general_utils.FormatData(input_df)\
        .unique(['A','B'])
    output_df = pd.DataFrame(
        {'A': [1, 1, 3],
         'B': [1,2, 5]})
    assert FD.data.equals(output_df)

    FD = general_utils.FormatData(input_df)\
        .unique(['A','C'])
    output_df = pd.DataFrame(
        {'A': [1, 3, 3],
         'C' : [np.nan,12, 13]},
         index=[0,2,3])
    assert FD.data.equals(output_df)


def test_FormatData_write_data():
    '''test FormatData.write_data'''
    # Wrapper for to_csv, to_stata
    assert True
