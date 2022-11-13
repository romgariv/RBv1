#! usr/bin/env python3
#
# Author:   Roman Rivera

'''pytest functions in merge_functions that require teardown/setup'''

import logging
import sys
import pandas as pd
import numpy as np
import pytest
logger = logging.getLogger('test')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s[%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S')

stream_out = logging.StreamHandler(sys.stdout)
stream_out.setFormatter(formatter)
logger.addHandler(stream_out)

from general_utils import FormatData
from merge_functions import MergeData, BASE_OD

# FULLY TEST MERGING FOR OTO

def test_oto():
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
                  'log': logger}
    auid_args = {'new_uid' : 'UID', 'start_uid' : 5, 'auid_args_dict' : {'id_cols' : ['data_id']}}

    FD = FormatData(input_df, **input_args)\
        .qfilter('merge == 1')\
        .dcolumn('merge')\
        .assign_unique_ids(**auid_args)\
        .reshape_long('star')\
        .add_columns([
                "F4FN",
                {'exec' : "_DATA_['L4LN'] = _DATA_['last_name_NS'].map(lambda x: x[-4:None])"}])

    def test_initialize_ref():
        '''test initializing ref'''
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
    test_initialize_ref()

    input_sup_df = pd.DataFrame(
        {'sid' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'birth_year' : [1970, 1990, 1985, 1965, 1986],
         'middle_initial': [np.nan, 'M', 'C', 'L', 'E'],
         'suffix_name': ['SR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'current_star': [np.nan, 51, 20, 100, 192]})
    input_sup_args = {'uid': 'sid',
                      'log': logger}

    first_four = lambda x: x[:4]
    FD2 = FormatData(input_sup_df, **input_sup_args)\
            .set_columns({'current_star' : 'star', 'sid' : 'SID'})\
            .add_columns([
                "F4FN",
                {'exec' : "_DATA_['L4LN'] = _DATA_['last_name_NS'].map(lambda x: x[-4:None])"},
                {'out_col' : 'F4LN', 'in_col' : 'last_name_NS', 'func' : first_four}])

    def test_initialize_sup():
        '''test initializing sup'''
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
    test_initialize_sup()

    MD = MergeData(FD, FD2, log=logger)

    def test_initializing_MergeData():
        '''test initializing MergeData'''
        assert MD.rdf.equals(FD.data)
        assert MD.ruid == FD.uid
        assert MD.sdf.equals(FD2.data)
        assert MD.suid == FD2.uid
        assert MD.mdf.empty
        assert MD.conflicts.empty
    test_initializing_MergeData()

    def test_generate_on_lists():
        '''test generate_on_list staticmethod'''
        results = MD.generate_on_lists(
            data_cols=list(set(MD.rdf.columns) & set(MD.sdf.columns)),
            base_OD=BASE_OD,
            custom_merges=[["first_name_NS", "L4LN", "middle_initial"],
                           {'cols' : ["F4FN", "star"], 'query' : 'F4FN=="KATH"'}],
            base_OD_edits=[])
        output_on_list = [
            ['star', 'first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name'],
            ['star', 'first_name_NS', 'last_name_NS', 'middle_initial'],
            ['star', 'first_name_NS', 'last_name_NS', 'suffix_name'],
            ['star', 'first_name_NS', 'last_name_NS'],
            ['star', 'F4FN', 'last_name_NS', 'middle_initial', 'suffix_name'],
            ['star', 'F4FN', 'last_name_NS', 'middle_initial'],
            ['star', 'F4FN', 'last_name_NS', 'suffix_name'],
            ['star', 'F4FN', 'last_name_NS'],
            ['first_name_NS', 'last_name_NS', 'middle_initial', 'suffix_name'],
            ['first_name_NS', 'last_name_NS', 'middle_initial'],
            ['first_name_NS', 'last_name_NS', 'suffix_name'],
            ['first_name_NS', 'last_name_NS'],
            ['F4FN', 'last_name_NS', 'middle_initial', 'suffix_name'],
            ['F4FN', 'last_name_NS', 'middle_initial'],
            ['F4FN', 'last_name_NS', 'suffix_name'],
            ['F4FN', 'last_name_NS'],
            ['first_name_NS', 'L4LN', 'middle_initial'],
            {'cols': ['F4FN', 'star'], 'query': 'F4FN=="KATH"'}
        ]
        assert results == output_on_list
    test_generate_on_lists()


    MD = MD.loop_merge(custom_merges=[["first_name_NS", "L4LN", "middle_initial"],
                                     {'cols' : ["F4FN", "star"],
                                      'query' : 'F4FN=="KATH"'}],
                      mode='oto')

    def test_initializing_loop_merge():
        ''' testing results of loop_merge'''
        mdf = pd.DataFrame({
            'UID' : [6, 5, 8, 7],
            'SID' : [2, 1, 4, 3],
            'matched_on' : ['star-first_name_NS-last_name_NS',
                            'first_name_NS-last_name_NS-suffix_name',
                            'first_name_NS-L4LN-middle_initial',
                            'F4FN=="KATH" : F4FN-star']},
        columns=['UID', 'SID', 'matched_on'], dtype='O')
        rudf = pd.DataFrame({
            'data_id' : [110, 110],
            'first_name_NS': ['DEAN', 'DEAN'],
            'middle_initial': ['E', 'E'],
            'suffix_name': pd.Series([np.nan, np.nan], dtype='O'),
            'last_name_NS': ['SMITH', 'SMITH'],
            'star' : [2.0, 3.0],
            'F4FN' : ['DEAN', 'DEAN'],
            'L4LN' : ['MITH', 'MITH'],
            'UID' : [9, 9]
        }, index=[7,8])
        sudf = pd.DataFrame({
            'SID' : [5],
            'first_name_NS': ['JENNA'],
            'birth_year' : [1986],
            'middle_initial': ['E'],
            'suffix_name': pd.Series([np.nan], dtype='O'),
            'last_name_NS': ['JONES'],
            'star': [192.0],
            'F4FN': ['JENN'],
            'L4LN': ['ONES'],
            'F4LN': ['JONE']}, index=[4])
        assert MD.rdf.equals(FD.data)
        assert MD.ruid == FD.uid
        assert MD.sdf.equals(FD2.data)
        assert MD.suid == FD2.uid
        assert MD.conflicts.empty
        assert MD.mdf.equals(mdf)
        assert set(MD.rudf.columns) == set(rudf.columns)
        assert MD.rudf.equals(rudf[MD.rudf.columns])
        assert set(MD.sudf.columns) == set(sudf.columns)
        assert MD.sudf.equals(sudf[MD.sudf.columns])
    test_initializing_loop_merge()


def test_event_loc_dfs():
    '''tests merging for not necessarily deduplicated event/location from dataframes'''
    input_df = pd.DataFrame(
    {'first_name': ['BOB', 'BOB', 'BOB', 'AN'],
     'last_name': ['JONES', 'JONES', 'JONES', 'KIM'],
     'unit': [1,2,3, 4],
     'ones' : [1,1,1, 1],
    'start' : pd.to_datetime(['2000-01-01', '2011-01-01', '2000-01-01', pd.NaT]),
    'end' : pd.to_datetime(['2010-01-01', pd.NaT, pd.NaT, pd.NaT]),
    'UID' : [1,1,2, 3]})


    input_sup_df = pd.DataFrame(
        {'first_name': ['BOB', 'BOB', 'BOB', 'BOB', 'AMY'],
             'last_name': ['JONES', 'JONES', 'JONES', 'JONES', 'KAY'],
             'unit': [1,1,4, 5, 6],
             'ones' : [1,1,1,1, 1],
            'event_date' : pd.to_datetime(['2001-01-01', '2001-02-01', '2000-02-01', '2000-01-01', pd.NaT]),
            'SID' : [10,12, 10, 13, 14]})


    MD = MergeData(input_df, input_sup_df, suid='SID', ruid='UID', log=logger)
    def test_initializing_MergeData():
        '''test initializing MergeData'''
        assert MD.rdf.equals(input_df)
        assert MD.ruid == 'UID'
        assert MD.sdf.equals(input_sup_df)
        assert MD.suid == 'SID'
        assert MD.mdf.empty
        assert MD.conflicts.empty
    test_initializing_MergeData()

    MD = MD.loop_merge(base_OD=[('first_name', ['first_name']), ('last_name', ['last_name'])],
                       base_OD_edits=[('ones', ['ones'])],
                      event_col='event_date', loc_start='start', loc_end='end', loc_cols=['unit'],
                      mode='otm')
    def test_initializing_loop_merge_event_True():
        ''''''
        mdf = pd.DataFrame({
            'UID' : [1, 1],
            'SID' : [10, 12],
            'matched_on' : ['first_name-last_name-ones-unit', 'first_name-last_name-ones-unit']
        }, columns=['UID', 'SID', 'matched_on'], dtype='O')
        sudf = pd.DataFrame({
            'first_name': ['BOB', 'AMY'],
            'last_name': ['JONES', 'KAY'],
            'ones' : [1, 1],
            'unit': [5, 6],
            'event_date' : pd.to_datetime(['2000-01-01', pd.NaT]),
            'SID' : [13, 14]},
            index=[3, 4],
            columns=['SID', 'event_date', 'first_name', 'last_name', 'ones', 'unit'])

        rudf = pd.DataFrame(
            {'first_name': ['BOB', 'AN'],
             'last_name': ['JONES', 'KIM'],
             'unit': [3, 4],
             'ones' : [1, 1],
            'start' : pd.to_datetime(['2000-01-01', pd.NaT]),
            'end' : pd.to_datetime([pd.NaT, pd.NaT]),
            'UID' : [2, 3]},
            index=[2, 3],
            columns=['UID', 'end', 'first_name', 'last_name', 'ones', 'start', 'unit'])
        assert set(mdf.columns) == set(MD.mdf.columns)
        assert MD.mdf.equals(mdf[MD.mdf.columns])
        assert set(sudf.columns) == set(MD.sudf.columns)
        assert MD.sudf.equals(sudf[MD.sudf.columns])
        assert set(rudf.columns) == set(MD.rudf.columns)
        assert MD.rudf.equals(rudf[MD.rudf.columns])
        assert MD.conflicts.empty
    test_initializing_loop_merge_event_True()
    MD = MD.loop_merge(base_OD=[('first_name', ['first_name']), ('last_name', ['last_name'])],
                       base_OD_edits=[('ones', ['ones'])])

    def test_second_loop_merge():
        mdf = pd.DataFrame({
            'UID' : [1, 1, 2],
            'SID' : [10, 12, 13],
            'matched_on' : ['first_name-last_name-ones-unit', 'first_name-last_name-ones-unit', 'first_name-last_name-ones']
        }, columns=['UID', 'SID', 'matched_on'], dtype='O')
        sudf = pd.DataFrame({
            'first_name': ['AMY'],
            'last_name': ['KAY'],
            'ones' : [1],
            'unit': [6],
            'event_date' : pd.to_datetime([pd.NaT]),
            'SID' : [14]},
            index=[4],
            columns=['SID', 'event_date', 'first_name', 'last_name', 'ones', 'unit'])

        rudf = pd.DataFrame(
            {'first_name': ['AN'],
             'last_name': ['KIM'],
             'unit': [4],
             'ones' : [1],
            'start' : pd.to_datetime([pd.NaT]),
            'end' : pd.to_datetime([pd.NaT]),
            'UID' : [3]},
            index=[3],
            columns=['UID', 'end', 'first_name', 'last_name', 'ones', 'start', 'unit'])
        assert set(mdf.columns) == set(MD.mdf.columns)
        assert MD.mdf.equals(mdf[MD.mdf.columns])
        assert set(sudf.columns) == set(MD.sudf.columns)
        assert MD.sudf.equals(sudf[MD.sudf.columns])
        assert set(rudf.columns) == set(MD.rudf.columns)
        assert MD.rudf.equals(rudf[MD.rudf.columns])
        assert MD.conflicts.empty
    test_second_loop_merge()

    def test_append_to_reference_keep_um_False():
        results = MD.append_to_reference(keep_um=False)
        output_df = pd.DataFrame({
            'first_name': ['BOB', 'BOB', 'BOB', 'BOB', 'BOB', 'BOB', 'BOB', 'AN'],
             'last_name': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'KIM'],
             'unit': [1,2, 1, 1, 4,3, 5, 4],
             'ones' : [1,1,1, 1, 1,1,1, 1],
            'start' : pd.to_datetime(['2000-01-01', '2011-01-01', pd.NaT, pd.NaT, pd.NaT, '2000-01-01', pd.NaT, pd.NaT]),
            'end' : pd.to_datetime(['2010-01-01', pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT]),
            'UID' : [1,1, 1, 1, 1, 2, 2, 3],
            'event_date' : pd.to_datetime([pd.NaT, pd.NaT, '2001-01-01', '2001-02-01', '2000-02-01', pd.NaT, '2000-01-01', pd.NaT]),
            'SID' : [np.nan, np.nan, 10,12, 10, np.nan, 13, np.nan]})
        assert set(results.columns) == set(output_df.columns)
        assert results.data.equals(output_df[results.columns])
        assert results.uid == 'UID'
    test_append_to_reference_keep_um_False()

    def test_append_to_reference_keep_um_True():
        results = MD.append_to_reference(keep_um=True)
        output_df = pd.DataFrame({
            'first_name': ['BOB', 'BOB', 'BOB', 'BOB', 'BOB', 'BOB', 'BOB', 'AN', 'AMY'],
             'last_name': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'KIM', 'KAY'],
             'unit': [1,2, 1, 1, 4,3, 5, 4, 6],
             'ones' : [1,1,1, 1, 1,1,1, 1, 1],
            'start' : pd.to_datetime(['2000-01-01', '2011-01-01', pd.NaT, pd.NaT, pd.NaT, '2000-01-01', pd.NaT, pd.NaT, pd.NaT]),
            'end' : pd.to_datetime(['2010-01-01', pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT]),
            'UID' : [1,1, 1, 1, 1, 2, 2, 3, 4],
            'event_date' : pd.to_datetime([pd.NaT, pd.NaT, '2001-01-01', '2001-02-01', '2000-02-01', pd.NaT, '2000-01-01', pd.NaT, pd.NaT]),
            'SID' : [np.nan, np.nan, 10,12, 10, np.nan, 13, np.nan, 14]})
        assert set(results.columns) == set(output_df.columns)
        assert results.data.equals(output_df[results.columns])
        assert results.uid == 'UID'
    test_append_to_reference_keep_um_True()

# TEST MODE TYPES
def test_oto_conflicts():
    FD1 = FormatData(
        pd.DataFrame({
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB'],
             'middle_initial': ['M', 'M', 'M', np.nan, np.nan],
             'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR'],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES'],
             'star' : [10, 20, 30, 50, 51],
             'UID' : [5, 5, 5, 6, 6]}),
        uid='UID',
        log=logger)
    FD2 = FormatData(
        pd.DataFrame({
            'SID' : [1, 2],
            'first_name_NS': ['BOB', 'BOB'],
            'middle_initial': [np.nan, 'M'],
            'suffix_name': ['SR', np.nan],
            'last_name_NS': ['JONES', 'JONES'],
            'star': [np.nan, 51]}),
        uid='SID',
        log=logger)
    MD = MergeData(FD1, FD2, log=logger)\
        .loop_merge(base_OD=None,
                    custom_merges=[["first_name_NS", "last_name_NS"]],
                   mode='oto')

    conflicts = pd.DataFrame({
        'SID': [1,2, 1, 2],
        'UID' : [5, 5, 6, 6],
        'matched_on' : ['first_name_NS-last_name_NS', 'first_name_NS-last_name_NS', 'first_name_NS-last_name_NS', 'first_name_NS-last_name_NS']},
        columns=['UID', 'SID', 'matched_on'],
        index=[0,1, 6, 7],
        dtype='O')
    assert MD.conflicts.equals(conflicts)
    assert MD.mdf.empty

def test_mtm_conflicts():
    FD1 = FormatData(
        pd.DataFrame({
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB'],
             'middle_initial': ['M', 'M', 'M', np.nan, np.nan],
             'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR'],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES'],
             'star' : [10, 20, 30, 50, 51],
             'UID' : [5, 5, 5, 6, 6]}),
        uid='UID',
        log=logger)
    FD2 = FormatData(
        pd.DataFrame({
            'SID' : [1, 2],
            'first_name_NS': ['BOB', 'BOB'],
            'middle_initial': [np.nan, 'M'],
            'suffix_name': ['SR', np.nan],
            'last_name_NS': ['JONES', 'JONES'],
            'star': [np.nan, 51]}),
        uid='SID',
        log=logger)
    MD = MergeData(FD1, FD2, log=logger)\
        .loop_merge(base_OD=None,
                    custom_merges=[["first_name_NS", "last_name_NS"]],
                   mode='mtm')

    mdf = pd.DataFrame({
        'SID': [1,2, 1, 2],
        'UID' : [5, 5, 6, 6],
        'matched_on' : ['first_name_NS-last_name_NS', 'first_name_NS-last_name_NS', 'first_name_NS-last_name_NS', 'first_name_NS-last_name_NS']},
        columns=['UID', 'SID', 'matched_on'],
        index=[0,1, 2, 3],
        dtype='O')
    assert MD.mdf.equals(mdf)
    assert MD.conflicts.empty

def test_mto_conflicts():
    FD1 = FormatData(
        pd.DataFrame({
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB'],
             'middle_initial' : [np.nan, np.nan, np.nan, 'M', 'M'],
             'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR'],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES'],
             'star' : [10, 20, 30, 50, 51],
             'UID' : [4, 5, 5, 6, 6]}),
        uid='UID',
        log=logger)
    FD2 = FormatData(
        pd.DataFrame({
            'SID' : [1, 2, 3],
            'first_name_NS': ['BOB', 'BOB', 'BOB'],
            'middle_initial': [np.nan, 'M', 'M'],
            'suffix_name': ['SR', 'JR', 'JR'],
            'last_name_NS': ['JONES', 'JONES', 'JONES']}),
        uid='SID',
        log=logger)
    MD = MergeData(FD1, FD2, log=logger)\
        .loop_merge(base_OD=None,
                    custom_merges=[["first_name_NS", "last_name_NS", "suffix_name"]],
                   mode='mto')

    conflicts = pd.DataFrame({
        'SID': [2, 3],
        'UID' : [6, 6],
        'matched_on' : ['first_name_NS-last_name_NS-suffix_name', 'first_name_NS-last_name_NS-suffix_name']
    }, index=[3,4], dtype='O', columns=['UID', 'SID', 'matched_on'])
    mdf = pd.DataFrame({
        'SID': [1, 1],
        'UID' : [4, 5],
        'matched_on' : ['first_name_NS-last_name_NS-suffix_name', 'first_name_NS-last_name_NS-suffix_name']
    }, index=[0,1], dtype='O', columns=['UID', 'SID', 'matched_on'])
    assert MD.mdf.equals(mdf)
    assert MD.conflicts.equals(conflicts)

def test_otm_conflicts():
    FD1 = FormatData(
        pd.DataFrame({
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB'],
             'middle_initial' : [np.nan, np.nan, np.nan, 'M', 'M'],
             'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR'],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES'],
             'star' : [10, 20, 30, 50, 51],
             'UID' : [4, 5, 5, 6, 6]}),
        uid='UID',
        log=logger)
    FD2 = FormatData(
        pd.DataFrame({
            'SID' : [1, 2, 3],
            'first_name_NS': ['BOB', 'BOB', 'BOB'],
            'middle_initial': [np.nan, 'M', 'M'],
            'suffix_name': ['SR', 'JR', 'JR'],
            'last_name_NS': ['JONES', 'JONES', 'JONES']}),
        uid='SID',
        log=logger)
    MD = MergeData(FD1, FD2, log=logger)\
        .loop_merge(base_OD=None,
                    custom_merges=[["first_name_NS", "last_name_NS", "suffix_name"]],
                   mode='otm')
    mdf = pd.DataFrame({
        'SID': [2, 3],
        'UID' : [6, 6],
        'matched_on' : ['first_name_NS-last_name_NS-suffix_name', 'first_name_NS-last_name_NS-suffix_name']
    }, index=[0,1], dtype='O', columns=['UID', 'SID', 'matched_on'])
    conflicts = pd.DataFrame({
        'SID': [1, 1],
        'UID' : [4, 5],
        'matched_on' : ['first_name_NS-last_name_NS-suffix_name', 'first_name_NS-last_name_NS-suffix_name']
    }, index=[0,1], dtype='O', columns=['UID', 'SID', 'matched_on'])
    assert MD.mdf.equals(mdf)
    assert MD.conflicts.equals(conflicts)


# TEST CYCLE OPTIONS
def test_cycle_sup():
    '''test for MergeData cycle = sup'''
    FD1 = FormatData(
        pd.DataFrame({
             'first_name_NS': ['BOB'],
             'suffix_name': ['JR'],
             'last_name_NS': ['JONES'],
             'UID' : [5]}),
        uid='UID',
        log=logger)
    FD2 = FormatData(
        pd.DataFrame({
            'SID' : [1, 2],
            'first_name_NS': ['BOB', 'BOB'],
            'suffix_name': ['SR', 'JR'],
            'last_name_NS': ['JONES', 'JONES']}),
        uid='SID',
        log=logger)

    MD = MergeData(FD1, FD2, log=logger)\
        .loop_merge(base_OD=None,
                    custom_merges=[["first_name_NS", "last_name_NS", "suffix_name"],
                                   ["first_name_NS", "last_name_NS"]],
                   cycle='sup')
    mdf = pd.DataFrame({
        'SID': [2, 1],
        'UID' : [5, 5],
        'matched_on' : ['first_name_NS-last_name_NS-suffix_name', 'first_name_NS-last_name_NS']
    }, index=[0,1], dtype='O', columns=['UID', 'SID', 'matched_on'])
    assert MD.mdf.equals(mdf)

def test_cycle_ref():
    FD1 = FormatData(
        pd.DataFrame({
             'first_name_NS': ['BOB', 'BOB'],
             'suffix_name': ['JR', 'SR'],
             'last_name_NS': ['JONES', 'JONES'],
             'UID' : [5,6]}),
        uid='UID',
        log=logger)
    FD2 = FormatData(
        pd.DataFrame({
            'SID' : [2, 2],
            'first_name_NS': ['BOB', 'BOB'],
            'suffix_name': [np.nan, 'JR'],
            'last_name_NS': ['JONES', 'JONES']}),
        uid='SID',
        log=logger)

    MD = MergeData(FD1, FD2, log=logger)\
        .loop_merge(base_OD=None,
                    custom_merges=[["first_name_NS", "last_name_NS", "suffix_name"],
                                   ["first_name_NS", "last_name_NS"]],
                   cycle='ref')
    mdf = pd.DataFrame({
        'SID': [2, 2],
        'UID' : [5, 6],
        'matched_on' : ['first_name_NS-last_name_NS-suffix_name', 'first_name_NS-last_name_NS']
    }, index=[0,1], dtype='O', columns=['UID', 'SID', 'matched_on'])
    assert MD.mdf.equals(mdf)

    FD1 = FormatData(
        pd.DataFrame({
             'first_name_NS': ['BOB', 'BOB'],
             'suffix_name': ['JR', 'SR'],
             'last_name_NS': ['JONES', 'JONES'],
             'UID' : [5,6]}),
        uid='UID',
        log=logger)
    FD2 = FormatData(
        pd.DataFrame({
            'SID' : [2, 2],
            'first_name_NS': ['BOB', 'BOB'],
            'suffix_name': [np.nan, 'JR'],
            'last_name_NS': ['JONES', 'JONES']}),
        uid='SID',
        log=logger)

    MD = MergeData(FD1, FD2, log=logger)\
        .loop_merge(base_OD=None,
                    custom_merges=[["first_name_NS", "last_name_NS", "suffix_name"],
                                   ["first_name_NS", "last_name_NS"]],
                   cycle='both')
    mdf = pd.DataFrame({
        'SID': [2],
        'UID' : [5],
        'matched_on' : ['first_name_NS-last_name_NS-suffix_name']
    }, index=[0], dtype='O', columns=['UID', 'SID', 'matched_on'])
    assert MD.mdf.equals(mdf)

def test_cycle_both_supcase():
    FD1 = FormatData(
        pd.DataFrame({
             'first_name_NS': ['BOB'],
             'suffix_name': ['JR'],
             'last_name_NS': ['JONES'],
             'UID' : [5]}),
        uid='UID',
        log=logger)
    FD2 = FormatData(
        pd.DataFrame({
            'SID' : [1, 2],
            'first_name_NS': ['BOB', 'BOB'],
            'suffix_name': ['SR', 'JR'],
            'last_name_NS': ['JONES', 'JONES']}),
        uid='SID',
        log=logger)

    MD = MergeData(FD1, FD2, log=logger)\
        .loop_merge(base_OD=None,
                    custom_merges=[["first_name_NS", "last_name_NS", "suffix_name"],
                                   ["first_name_NS", "last_name_NS"]],
                   cycle='both')
    mdf = pd.DataFrame({
        'SID': [2],
        'UID' : [5],
        'matched_on' : ['first_name_NS-last_name_NS-suffix_name']
    }, index=[0], dtype='O', columns=['UID', 'SID', 'matched_on'])
    assert MD.mdf.equals(mdf)


# def test_conflict_groups():
#     FD1 = FormatData(
#         pd.DataFrame({
#              'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB'],
#              'middle_initial': ['M', 'M', 'M', np.nan, np.nan],
#              'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR'],
#              'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES'],
#              'star' : [10, 20, 30, 50, 51],
#              'UID' : [5, 5, 5, 6, 6]}),
#         uid='UID',
#         log=logger)
#     FD2 = FormatData(
#         pd.DataFrame({
#             'SID' : [1, 2],
#             'first_name_NS': ['BOB', 'BOB'],
#             'middle_initial': [np.nan, 'M'],
#             'suffix_name': ['SR', np.nan],
#             'last_name_NS': ['JONES', 'JONES'],
#             'star': [np.nan, 51]}),
#         uid='SID',
#         log=logger)
#     MD = MergeData(FD1, FD2, log=logger)\
#         .loop_merge(base_OD=None,
#                     custom_merges=[{'cols' : ["first_name_NS", "last_name_NS"],
#                                     'query' : 'first_name_NS == "BOB"'}],
#                    mode='oto')
#     grouped_conflicts = pd.DataFrame({
#         'UID' : [5, 5, 6, 6],
#         'SID' : [1, 2, 1, 2],
#         'matched_on' : ['first_name_NS-last_name_NS', 'first_name_NS-last_name_NS', 'first_name_NS-last_name_NS', 'first_name_NS-last_name_NS'],
#         'CGID' : [1,1,1,1]
#     }, columns=['UID', 'SID', 'matched_on', 'CGID'], index=[0, 1, 6, 7], dtype='O')
#     grouped_conflicts['CGID'] = grouped_conflicts['CGID'].astype(int)
#     assert MD.conflict_groups.equals(grouped_conflicts)

# Adapted tests from old merge_functions
def test_one_to_one_True():
    '''tests merging for one_to_one merging'''
    input_ref_df = pd.DataFrame(
        {'data_id' : [10, 10, 10, 20, 20, 30, 109],
         'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATHLEEN', 'ELLEN'],
         'middle_initial': ['M', 'M', 'M', np.nan, np.nan, np.nan, 'L'],
         'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR', np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY'],
         'F4FN': ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATH', 'ELLE'],
         'L4LN': ['ONES', 'ONES', 'ONES', 'ONES', 'ONES', 'MITH', 'IELY'],
         'star' : [10, 20, 30, 50, 51, 20, np.nan],
         'UID' : [5, 5, 5, 6, 6, 7, 8]})
    input_sup_df = pd.DataFrame(
        {'SID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'F4FN' : ['BOB', 'BOB', 'KATH', 'ELLE', 'JENN'],
         'birth_year' : [1970, 1990, 1985, 1965, 1986],
         'middle_initial': [np.nan, 'M', 'C', 'L', 'E'],
         'suffix_name': ['SR', np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'L4LN': ['ONES', 'ONES' , 'RANT', 'IELY', 'ONES'],
         'current_star': [np.nan, 51, 20, 100, 192],
         'star': [np.nan, 51, 20, 100, 192]})
    RD = MergeData(input_ref_df, ruid='UID', log=logger)\
        .add_sup_data(input_sup_df, suid='SID')

    RD = RD.loop_merge(custom_merges=[["first_name_NS", "L4LN", "middle_initial"],
                                     {'cols' : ["F4FN", "star"],
                                      'query' : 'F4FN=="KATH"'}])

    def test_loop_merge_merged_df():
        '''test merged data from loop_merge'''
        output_merged_df = pd.DataFrame(
            {'UID' : [6, 5, 8, 7],
             'SID' : [2, 1, 4, 3],
             'matched_on' : ['star-first_name_NS-last_name_NS',
                             'first_name_NS-last_name_NS-suffix_name',
                             'first_name_NS-L4LN-middle_initial',
                             'F4FN=="KATH" : F4FN-star']

            }).astype(str)
        results = RD.mdf.astype(str)
        output_merged_df = output_merged_df[results.columns]
        assert results.equals(output_merged_df)
    test_loop_merge_merged_df()

    def test_loop_merge_ref_um():
        '''test unmerged reference data from loop_merge'''
        results = RD.rudf
        assert results.empty
    test_loop_merge_ref_um()

    def test_loop_merge_sup_um():
        '''test unmerged supplementary data from loop_merge'''
        output_sup_um = pd.DataFrame(
            [{'SID' : 5,
             'first_name_NS' : 'JENNA',
             'last_name_NS' : 'JONES',
             'middle_initial' : 'E',
             'star' : 192.0,
             'current_star' : 192.0,
             'birth_year' : 1986,
             'suffix_name' : np.nan,
             'F4FN' : 'JENN',
             'L4LN' : 'ONES'}], index=[4])
        results = RD.sudf.astype(str)
        output_sup_um = output_sup_um[results.columns].astype(str)
        assert results.equals(output_sup_um)
    test_loop_merge_sup_um()

    RD = RD.append_to_reference(drop_cols=['current_star', 'F4FN', 'L4LN'], inplace=True)

    def test_append_to_reference_ref_df_drop_cols():
        '''test reference data after append_to_reference'''
        output_ref_df = pd.DataFrame(
            {'data_id' : [10, 10, 10, 20, 20, 30, 109,
                          np.nan, np.nan, np.nan, np.nan, np.nan],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'BOB','BOB', 'KATHLEEN', 'ELLEN',
                               'BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
             'middle_initial': ['M', 'M', 'M', np.nan, np.nan, np.nan, 'L',
                                np.nan, 'M', 'C', 'L', 'E'],
             'suffix_name': ['SR', 'SR', 'SR', 'JR', 'JR', np.nan, np.nan,
                             'SR', np.nan, np.nan, np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY',
                              'JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
             'star' : [10, 20, 30, 50, 51, 20, np.nan,
                       np.nan, 51, 20, 100, 192],
             'birth_year': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
                            1970, 1990, 1985, 1965, 1986],
             'UID' : [5, 5, 5, 6, 6, 7, 8,
                      5, 6, 7, 8, 9],
             'SID' : [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan,
                               1, 2, 3, 4, 5]})\
        .sort_values('UID')\
        .reset_index(drop=True)

        results = RD.rdf
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_append_to_reference_ref_df_drop_cols()

    def test_remerge_to_file():
        csv_opts = {'index': False}
        input_df = pd.DataFrame({
            'SID' :[1,2,3,4,5,6],
            'event' : [1, 0, 0, 0, 0, 1]
        }, columns=['SID', 'event'])
        input_path = 'test_remerge_to_file_input.csv'
        output_path = 'test_remerge_to_file_output.csv'
        test_output_path = 'test_remerge_to_file_test.csv'
        input_df.to_csv(input_path, **csv_opts)
        RD.remerge_to_file(input_path, output_path, csv_opts)

        output_df = pd.DataFrame({
            'SID' :[1,2,3,4,5,6],
            'event' : [1, 0, 0, 0, 0, 1],
            'UID' : [5,6,7,8,9,np.nan]
        }, columns=['SID', 'event', 'UID'])
        output_df.to_csv(test_output_path, **csv_opts)
        import filecmp
        assert filecmp.cmp(test_output_path,
                           output_path)
        import os
        os.system('rm %s %s %s'
                  % (input_path, output_path, test_output_path))
    test_remerge_to_file()


def test_one_to_one_False():
    '''tests merging for one_to_many merging'''
    input_ref_df = pd.DataFrame(
        {'data_id' : [10, 10, 10, 30, 109],
         'first_name_NS': ['BOB', 'BOB', 'BOB', 'KATHLEEN', 'ELLEN'],
         'middle_initial': ['M', 'M', 'M', np.nan, 'L'],
         'suffix_name': ['JR', 'JR', 'JR', np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY'],
         'star' : [10, 20, 30, 20, np.nan],
         'F4FN': ['BOB', 'BOB', 'BOB', 'KATH', 'ELLE'],
         'L4LN': ['ONES', 'ONES', 'ONES', 'MITH', 'IELY'],
         'UID' : [1, 1, 1, 2, 3]})

    input_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3, 4, 5],
         'first_name_NS': ['BOB', 'BOB', 'KATHY', 'ELLEN', 'JENNA'],
         'F4FN' : ['BOB', 'BOB', 'KATH', 'ELLE', 'JENN'],
         'middle_initial': ['M', np.nan, np.nan, 'L', 'E'],
         'suffix_name': [np.nan, 'JR', np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY', 'JONES'],
         'L4LN': ['ONES', 'ONES' , 'RANT', 'IELY', 'ONES'],
         'current_star': [np.nan, 51, 20, 100, 192],
         'star': [10, 20, 20, 100, 192]})
    RD = MergeData(input_ref_df, input_sup_df, ruid='UID',
                   suid='sub__2016_ID', log=logger)
    RD = RD.loop_merge(custom_merges=[['F4FN', 'star']],
                        base_OD = [
                            ('star',['star', '']),
                            ('first_name',['first_name_NS', 'F4FN']),
                            ('last_name', ['last_name_NS', 'L4LN']),
                            ('middle_initial', ['middle_initial', '']),
                            ('suffix_name',['suffix_name', ''])],
                       mode='otm', cycle='sup')

    def test_loop_merge_merged_df():
        '''test merged data from loop_merge'''
        output_merged_df = pd.DataFrame(
            {'UID' : [1, 1, 3, 2],
             'sub__2016_ID' : [1, 2, 4, 3],
             'matched_on' : ['star-first_name_NS-last_name_NS-middle_initial',
                             'star-first_name_NS-last_name_NS-suffix_name',
                             'first_name_NS-L4LN-middle_initial',
                             'F4FN-star']

            }).astype(str)
        results = RD.mdf.astype(str)
        output_merged_df = output_merged_df[results.columns]
        assert results.equals(output_merged_df)
    test_loop_merge_merged_df()

    def test_loop_merge_ref_um():
        '''test unmerged reference data from loop_merge'''
        output_ref_df = pd.DataFrame({'data_id' : [10, 10, 10, 30, 109],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'KATHLEEN', 'ELLEN'],
             'middle_initial': ['M', 'M', 'M', np.nan, 'L'],
             'suffix_name': ['JR', 'JR', 'JR', np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY'],
             'star' : [10, 20, 30, 20, np.nan],
             'F4FN': ['BOB', 'BOB', 'BOB', 'KATH', 'ELLE'],
             'L4LN': ['ONES', 'ONES', 'ONES', 'MITH', 'IELY'],
             'UID' : [1, 1, 1, 2, 3]})

        results = RD.rdf
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
        assert RD.rudf.empty
    test_loop_merge_ref_um()


    def test_loop_merge_sup_um():
        '''test unmerged supplementary data from loop_merge'''
        output_sup_um = pd.DataFrame(
            [{'sub__2016_ID' : 5,
             'first_name_NS' : 'JENNA',
             'last_name_NS' : 'JONES',
             'middle_initial' : 'E',
             'star' : 192,
             'current_star' : 192.0,
             'suffix_name' : np.nan,
             'F4FN' : 'JENN',
             'L4LN' : 'ONES'}], index=[4])

        results = RD.sudf.astype(str)
        output_sup_um = output_sup_um[results.columns].astype(str)
        assert results.equals(output_sup_um)
    test_loop_merge_sup_um()


    RD = RD.append_to_reference(keep_um=False, inplace=True,
                                drop_cols=["current_star", "F4FN", "L4LN"])


    def test_append_to_reference_ref_df_keep_sup_um():
        '''test reference data after append_to_reference'''
        output_ref_df = pd.DataFrame(
            {'data_id' : [10, 10, 10, 30, 109,
                          np.nan, np.nan, np.nan, np.nan],
             'first_name_NS': ['BOB', 'BOB', 'BOB', 'KATHLEEN', 'ELLEN',
                               'BOB', 'BOB', 'KATHY', 'ELLEN'],
             'middle_initial': ['M', 'M', 'M', np.nan, 'L',
                                'M', np.nan, np.nan, 'L'],
             'suffix_name': ['JR', 'JR', 'JR', np.nan, np.nan,
                             np.nan, 'JR', np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY',
                              'JONES', 'JONES' , 'GRANT', 'SKARNULISORIELY'],
             'star' : [10, 20, 30, 20, np.nan,
                       10, 20, 20, 100],
             'UID' : [1, 1, 1, 2, 3,
                      1, 1, 2, 3],
             'sub__2016_ID' : [np.nan, np.nan, np.nan, np.nan, np.nan,
                               1, 2, 3, 4]})\
            .sort_values('UID')\
            .reset_index(drop=True)
        results = RD.rdf
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_append_to_reference_ref_df_keep_sup_um()

def test_fill_data():
    '''tests merging for fill_data merging'''
    input_sup_df = pd.DataFrame(
        {'sub__2016_ID' : [1, 2, 3],
         'first_name_NS': ['BOB',  'AN', 'AN'],
         'middle_initial': [np.nan, 'L', 'L'],
         'suffix_name': [np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES', 'ORIELY', 'SMITH'],
         'current_star': [50, 20, np.nan],
         'star': [50, 20, np.nan]})
    SD = FormatData(input_sup_df, uid='sub__2016_ID', log=logger)\
        .fill_data(fill_cols=['first_name_NS', 'middle_initial',
                      'suffix_name', 'last_name_NS', 'star'])\
        .add_columns(["F2FN"])
    input_ref_df = pd.DataFrame(
        {'UID' : [1,1, 2, 2, 2, 2, 3],
         'first_name_NS': ['BOB','BOB', 'AN', 'AN', 'ANNA','ANNA', 'ANNIE'],
         'middle_initial': ['M','M', 'L', 'L', 'L', 'L', 'L'],
         'suffix_name': ['JR','JR', np.nan, np.nan, np.nan, np.nan, np.nan],
         'last_name_NS': ['JONES','JONES', 'SMITH', 'ORIELY', 'SMITH', 'ORIELY', 'SMITH'],
         'F2FN': ['BO','BO', 'AN', 'AN', 'AN','AN', 'AN'],
         'star' : [10.0, 50.0, 20.0, 20.0, 20.0, 20.0, 2.0]})

    RD = MergeData(input_ref_df, SD,
                   ruid='UID')\
    .loop_merge(custom_merges=[['F2FN', 'last_name_NS', 'middle_initial']])

    def test_loop_merge_merged_df():
        '''test merged data from loop_merge'''
        output_merged_df = pd.DataFrame(
            {'UID' : [2, 1,3],
             'sub__2016_ID' : [2, 1,3],
             'matched_on' : ['star-first_name_NS-last_name_NS-middle_initial',
                             'star-first_name_NS-last_name_NS',
                             'F2FN-last_name_NS-middle_initial']

            }).astype(str)
        results = RD.mdf.astype(str)
        output_merged_df = output_merged_df[results.columns]
        assert results.equals(output_merged_df)
    test_loop_merge_merged_df()

    def test_loop_merge_ref_um():
        '''test unmerged reference data from loop_merge'''
        results = RD.rudf
        assert results.empty
    test_loop_merge_ref_um()

    def test_loop_merge_sup_um():
        '''test unmerged supplementary data from loop_merge'''
        results = RD.sudf
        assert results.empty
    test_loop_merge_sup_um()

    RD = RD.append_to_reference(inplace=True, keep_um=True)

    def test_append_to_reference_ref_df():
        '''test reference data after append_to_reference'''
        output_ref_df = pd.DataFrame(
            {'first_name_NS': ['BOB', 'BOB', 'BOB', 'AN', 'AN', 'ANNA', 'ANNA', 'AN', 'ANNIE', 'AN'],
             'F2FN' : ['BO', 'BO', 'BO', 'AN', 'AN', 'AN', 'AN', 'AN', 'AN', 'AN'],
             'middle_initial': ['M', 'M', np.nan, 'L', 'L', 'L', 'L', 'L', 'L', 'L'],
             'suffix_name': ['JR', 'JR', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
             'last_name_NS': ['JONES', 'JONES', 'JONES', 'SMITH', 'ORIELY', 'SMITH', 'ORIELY', 'ORIELY', 'SMITH', 'SMITH'],
             'star' : [10, 50, 50, 20, 20, 20, 20, 20, 2, np.nan],
             'UID' : [1, 1, 1, 2, 2, 2, 2, 2, 3, 3],
             'sub__2016_ID' : [np.nan, np.nan, 1, np.nan, np.nan, np.nan, np.nan, 2, np.nan, 3]})\
            .sort_values('UID')\
            .reset_index(drop=True)
        results = RD.rdf
        output_ref_df = output_ref_df[results.columns]
        assert results.equals(output_ref_df)
    test_append_to_reference_ref_df()

# def test_final_profiles():
#     '''test final profiles creation'''
#     input_df = pd.DataFrame({
#         'ID' : [1, 1, 1, 2, 2, 3, 3],
#         'name' : ['MIKE', 'MICHAEL', 'MICHAEL', 'JANE', 'JAN', 'BOB', 'BOB'],
#         'age' : [25, 24, 24, 30, 31, 40, 42],
#         'rank': ['SGT', 'PO', 'PO', 'DET', 'PO', 'PO', 'PA'],
#         'fid1__2016-09_ID': [np.nan, np.nan, 10, np.nan, 34, 40, np.nan],
#         'fid2__2017-01_ID':[51, np.nan, np.nan, 13, np.nan, np.nan, np.nan],
#         'fid3__2015-01_ID': [np.nan, 2, np.nan, np.nan, np.nan, np.nan, 10111]
#     })
#     input_args = {
#     'aggregate_data_args' : {
#         'current_cols': ['rank'],
#         'time_col' : 'foia_date',
#         'mode_cols': ['name'],
#         'max_cols': ['age']},
#     'column_order' : ['current_rank', 'age', 'name']}
#     output_df = pd.DataFrame({
#         'ID' : [1,2,3],
#         'current_rank' : ['SGT', 'DET', 'PO'],
#         'age' : [25, 31, 42],
#         'name' : ['MICHAEL', 'JAN', 'BOB'],
#         'profile_count' : [3, 2, 2]},
#         columns=['ID', 'current_rank', 'age', 'name', 'profile_count'])
#
#     results = ReferenceData(input_df, uid='ID', log=log)\
#         .final_profiles(**input_args)\
#         .profiles
#     assert results.equals(output_df)
