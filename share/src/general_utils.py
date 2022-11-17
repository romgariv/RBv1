#!usr/bin/env python3
#
# Author(s):  Roman Rivera

'''script containing utility functions used for general purposes'''

import re
import csv
import copy
import itertools
import pandas as pd
import numpy as np
pd.options.display.max_columns=20

from inspect import getsourcelines

'''FORMAT DATA CLASS'''
class FormatData:
    def __init__(self, data=None, log=None, uid=None):
        if not data is None and not uid is None:
            assert uid in data.columns
        self.log = log if not log is None else get_basic_logger()
        self._uid = uid
        self._data = copy.deepcopy(data)

    @property
    def columns(self):
        return self._data.columns

    @columns.setter
    def columns(self, column_names):
        old_cols = self.columns
        self._uid = column_names[old_cols.find(self.uid)]
        self._data.columns = column_names
        log_str = ' , '.join(['%s -> %s' % (oc, nc)
                              for oc, nc in zip(old_cols, self.columns)
                              if oc != nc])
        self.log.info(log_str)

    @property
    def uid(self):
        assert self._uid in self._data.columns, 'No uid has been assigned'
        return self._uid
    @uid.setter
    def uid(self, new_uid):
        assert new_uid in self.columns, 'New uid not in columns'
        self._uid = new_uid
        return self

    @property
    def data(self):
        return self._data

    @property
    def shape(self):
        return self._data.shape

    @property
    def nrows(self):
        return self._data.shape[0]

    def head(self,n=5):
        return self._data.head(n)

    def tail(self,n=5):
        return self._data.tail(n)

    def set_columns(self, column_names):
        old_cols = self.columns.tolist()
        if isinstance(column_names, dict):
            if self._uid in column_names.keys():
                self._uid = column_names[self.uid]
            self._data.rename(columns=column_names, inplace=True)
        elif isinstance(column_names, list):
            if not self._uid is None:
                self._uid = column_names[old_cols.index(self.uid)]
            self._data.columns = column_names
        log_str = ' , '.join(['%s -> %s' % (oc, nc)
                              for oc, nc in zip(old_cols, self.columns)
                              if oc != nc])
        self.log.info(log_str)
        return self

    def LOG(self, message):
        self.log.info(message)
        return self

    def lookup(self, uids, inplace=False):
        assert not self._uid is None
        if not isinstance(uids, list):
            uids = [uids]
        lu_df = self._data[self._data[self._uid].isin(uids)].sort_values(self._uid)
        self.log.info("Selecting %s uids from %s list, with %d rows"
                      % (lu_df[self._uid].unique(), uids, lu_df.shape[0]))
        if inplace:
            self._data = lu_df
            return self
        else:
            return lu_df

    def qfilter(self, query, inplace=True):
        rows = self._data.shape[0]
        qf_data = self._data.query(query)
        self.log.info('%d rows dropped by filter query: %s' %(rows - qf_data.shape[0], query))
        if inplace:
            self._data = qf_data
            return self
        else:
            return qf_data

    def date_to_month(self, date_col, new_col, to_datetime=True, inplace=True):
        pre_nans = self._data[date_col].isnull().sum()
        month_col = pd.to_datetime(self._data[date_col].astype(str), errors='coerce')
        month_col[month_col.notnull()] = \
            month_col[month_col.notnull()].astype(str).str.slice(0, 8) + '01'
        if to_datetime:
            month_col = pd.to_datetime(month_col)
        else:
            month_col[month_col.isnull()] = np.nan
        post_nans = month_col.isnull().sum()
        self.log.info("%d pre-clean NAs, %d post-clean NAs" % (pre_nans, post_nans))
        issues = month_col[
            (month_col.isnull() & self._data[date_col].notnull()) |
            (month_col.astype(str).str.len() != 10)
        ]
        if issues.size != 0:
            self.log.warning("Some issues, converted to NA or incorrect length: %s" % (issues))

        if inplace:
            self._data[new_col] = month_col
            return self
        else:
            return month_col

    def dropna(self, dna_args, inplace=True):
        rows = self._data.shape[0]
        dna_data = self._data.dropna(**dna_args)
        self.log.info('%d rows dropped by dropna: %s' %(rows - dna_data.shape[0], dna_args))
        if inplace:
            self._data = dna_data
            return self
        else:
            return dna_data
    #
    # def filter(self, exec_str, inplace=True):
    #     rows = self._data.shape[0]
    #     exec_str = add_col['exec'].replace('_DATA_', 'self._data')
    #     self.log.info('Adding column by exec("%s")', exec_str)
    #     assert not any(cond in add_col['exec']
    #                    for cond in ['import', 'exec', 'eval'])
    #     exec(exec_str)
    #     f_data = self._data[
    #     self.log.info('%d rows dropped by filter query: %s' %(rows - qf_data.shape[0], query))
    #     if inplace:
    #         self._data = qf_data
    #         return self
    #     else:
    #         return qf_data

    def kcolumn(self, col, inplace=True):
        if isinstance(col, str):
            col = [col]
        if not self._uid is None and self._uid not in col:
            col = [self._uid] + col
        kc_data = self._data[col]
        self.log.info('Keeping column(s): %s' % (kc_data.columns.tolist()))
        if inplace:
            self._data = kc_data
            return self
        else:
            return kc_data

    def dcolumn(self, col, inplace=True):
        dc_data = self._data.drop(col, axis=1)
        self.log.info('Keeping column(s): %s' % (dc_data.columns.tolist()))
        if inplace:
            self._data = dc_data
            return self
        else:
            return dc_data

    def sort(self, cols=None, reverse=False, inplace=True, reset_index=True):
        if cols is None:
            cols = self.uid
        s_data = self._data.sort_values(cols, ascending= (not reverse))

        if reset_index:
            s_data = s_data.reset_index(drop=True)

        if inplace:
            self._data = s_data
            return self
        else:
            return s_data

    def import_data(self, in_file, uid=None, sheets=None, keep_columns=None,
                    add_row_id=True, column_names=None, inplace=True,
                    strip_colnames=True, skiprows=None, concat_axis=None,
                    no_header=False, dall_nan=True, permit_missing_cols=False,
                    encoding=None, sep=None, avoid_quote=False, skip_standard_cols=[]):

        if isinstance(in_file, str):
            self.log.info("Beginning import_data from file(s): %s" % (in_file))
            if not skiprows is None:
                self.log.info("Skipping first %d rows" % skiprows)

            if 'xls' in in_file.split('.')[-1]:
                header =  (None if no_header else 0)

                if sheets is None:
                    data = pd.read_excel(in_file, skiprows=skiprows,
                                         header=header)
                elif isinstance(sheets, list):
                    xls = pd.ExcelFile(in_file)
                    from functools import reduce
                    if concat_axis is None:
                        data = reduce(
                            lambda left,right:
                                pd.merge(left, right,
                                         on=list(set(left.columns.values) & set(right.columns))),
                            [pd.read_excel(xls, sheet_name=sheet, header=header) for sheet in sheets]
                        )
                    else:
                        data = pd.DataFrame()
                        for sheet in sheets:
                            tdf = pd.read_excel(xls, sheet_name=sheet, header=header, skiprows=skiprows)
                            tdf['SHEET'] = sheet
                            skip_standard_cols.append('SHEET')
                            data = pd.concat([data, tdf], axis=concat_axis, sort=False)
                else:
                    data = pd.read_excel(in_file, sheet_name=sheets, skiprows=skiprows,
                                         header=header)

            elif any('csv' == x for x in in_file.split('.')[-2:]):
                data = pd.read_csv(in_file, skiprows=skiprows,
                                   header = (None if no_header else 'infer'),
                                   encoding=encoding)
            elif in_file.split('.')[-1] == 'txt' or in_file.split('.')[-2]=='txt':
                data = pd.read_table(
                    in_file, sep=sep, encoding=encoding, skiprows=skiprows,
                    header=(None if no_header else 'infer'),
                    quoting=(csv.QUOTE_NONE if avoid_quote else 0))
            elif in_file.split('.')[-1] == 'dta':
                data = pd.read_stata(in_file)
            else:
                raise "What type of file?"
        elif isinstance(in_file, list):
            header =  (None if no_header else 0)
            if concat_axis is None:
                from functools import reduce
                data = reduce(
                    lambda left,right:
                        pd.merge(left, right,
                                 on=list(set(left.columns.values) & set(right.columns))),
                    [pd.read_csv(infile, header=header) for infile in in_file]
                )
            else:
                datas = []
                for infile in in_file:
                    if infile.split('.')[-1] in ['xlsx', 'xls']:
                        tdf = pd.read_excel(infile, header=header, skiprows=skiprows)
                    else:
                        tdf = pd.read_csv(infile, header=header, skiprows=skiprows)
                    if dall_nan:
                        old_rows = tdf.shape[0]
                        tdf = tdf.dropna(how='all', axis=0)
                        if tdf.shape[0] != old_rows:
                            self.log.info("Dropped %d all nan rows" % (old_rows - tdf.shape[0]))
                    tdf['FILE'] = infile
                    skip_standard_cols.append('FILE')
                    datas.append(tdf)
                data = pd.concat(datas, axis=concat_axis, sort=False)
        else:
            assert isinstance(in_file, pd.DataFrame)
            self.log.info("Beginning import_data from dataframe with head(2): \n%s" % in_file.head(2))
            data = in_file

        if dall_nan:
            old_rows = data.shape[0]
            data = data.dropna(how='all', axis=0)
            if data.shape[0] != old_rows:
                self.log.info("Dropped %d all nan rows" % (old_rows - data.shape[0]))

        data = data.reset_index(drop=True)
        self.log.info("Data shape = %d rows, %d columns" % tuple(data.shape))
        if strip_colnames and not no_header:
            data.columns = [re.sub(r"^\s+|\s+$|\s+(?=\s)", "", x)
                            for x in data.columns]

        if not keep_columns is None:
            self.log.info('Keeping columns: %s' % keep_columns)
            data = data[keep_columns]

        if not column_names is None:
            old_cols = [col for col in data.columns if col not in skip_standard_cols]
            if isinstance(column_names, str):
                if not skip_standard_cols:
                    data.columns = standardize_columns(old_cols, column_names,
                                                    permit_missing_cols=permit_missing_cols)
                else:
                    cols_dict = dict(zip(old_cols,
                                        standardize_columns(old_cols, column_names,
                                              permit_missing_cols=permit_missing_cols)))
                    for ssc in skip_standard_cols:
                        cols_dict[ssc] = ssc
                    data.rename(columns=cols_dict, inplace=True)
            elif isinstance(column_names, dict):
                data.rename(columns=column_names, inplace=True)
            elif isinstance(column_names, list):
                data.columns = column_names
            elif callable(column_names):
                data.columns = [column_names(c) for c in data.columns]
            else:
                raise "What column_names?"
            log_str = ' , '.join(['%s -> %s' % (oc, nc) for oc, nc in zip(old_cols, data.columns)])
            self.log.info(log_str)

        if add_row_id and 'ROWID' not in data.columns:
            data.insert(0, 'ROWID', data.index + 1)
            self.log.info("ROWID column inserted")
        if inplace:
            self._data = data
            if not uid is None:
                assert uid in data.columns, print('%s not in %s' %(uid, data.columns))
                self._uid = uid
            return self
        else:
            return data

    def write_data(self, outfile, stata_opts={}, index=False):
        log_str = 'Writing data with %d rows to %s' % (self._data.shape[0], outfile)
        if not self._uid is None:
            log_str = log_str + (' with %d unique UIDs' % self._data[self._uid].nunique())
        self.log.info(log_str)
        if outfile[-3:] == '.gz':
            self._data.to_csv(outfile, compression='gzip', index=index)
        elif outfile[-4:] == '.dta':
            self._data.to_stata(outfile, write_index=index, **stata_opts)
        else:
            self._data.to_csv(outfile, index=index)
        return self

    def reshape_long(self, wide_col, id_col=None, inplace=True):
        if id_col is None:
            assert not self._uid is None
            id_col = self._uid
        rows = self._data.shape[0]
        reshaped_df = reshape_data(self._data, wide_col, id_col)\
            .sort_values(id_col)\
            .reset_index(drop=True)
        self.log.info('Data reshaped wide (%d rows) '
                          'to long (%d rows) by %s columns',
                          rows, reshaped_df.shape[0], wide_col)
        if inplace:
            self._data = reshaped_df
            return self
        else:
            return reshaped_df

    def fill_data(self, fill_cols, inplace=True):
        assert not self._uid is None
        self.log.info('Beginning fill_data() on ref_df for %s columns'
                      ' by %s, with initial rows = %d',
                      fill_cols, self._uid, self._data.shape[0])
        filled_df = fill_data(self._data[[self._uid] + fill_cols], self._uid)
        self.log.info('fill_data() complete, final rows = %d', filled_df.shape[0])
        if inplace:
            self._data = filled_df
            return self
        else:
            return filled_df

    def add_columns(self, add_cols):
        for add_col in add_cols:
            if isinstance(add_col, dict):
                if 'exec' in add_col.keys():
                    exec_str = add_col['exec'].replace('_DATA_', 'self._data')
                    self.log.info('Adding column by exec("%s")', exec_str)
                    assert not any(cond in add_col['exec']
                                   for cond in ['import', 'exec', 'eval'])
                    exec(exec_str)
                elif set(add_col.keys()) & set(['func', 'out_col', 'in_col']):
                    assert (callable(add_col['func']) and
                            isinstance(add_col['in_col'],str) and
                            isinstance(add_col['out_col'],str))
                    self.log.info('Adding column %s = %s.map(%s)'
                                  %(add_col['out_col'], add_col['in_col'],
                                    getsourcelines(add_col['func'])[0][0][:-1]))
                    self._data[add_col['out_col']] = self._data[add_col['in_col']].map(add_col['func'])

            elif isinstance(add_col, str):
                if re.search("[F|L][0-9][F|L]N", add_col):
                    use_col = 'first_name_NS' if add_col[2] == 'F'\
                               else 'last_name_NS'
                    start, end = (0, int(add_col[1])) if add_col[0] == 'F'\
                                 else (-int(add_col[1]), None)
                    self._data[add_col] = self._data[use_col].map(lambda x: x[start:end])
        return self



    def recode(self, old_col, new_col, replace_dict, missing_na=True):
        assert isinstance(replace_dict, dict)
        self.log.info("Recoding %s to %s" % (old_col, new_col))

        if any(isinstance(vals, list) for vals in replace_dict.values()):
            flat_values = []
            for vals in replace_dict.values():
                if isinstance(vals, list): flat_values.extend(vals)
                else: flat_values.append(vals)
            assert len(flat_values) == len(set(flat_values)), 'Some value lists contain duplicates'

            for k, vals in replace_dict.items():
                if isinstance(vals, list):
                    self._data.loc[self.data.index[self._data[old_col].isin(vals)],
                                   "NEW"] = k
                else:
                    self._data.loc[self.data.index[self._data[old_col] == vals],
                                   "NEW"] = k
            if not missing_na:
                locs = ~self._data[old_col].isin(flat_values)
                self._data.loc[locs,"NEW"] = self._data.loc[locs,old_col]

        else:
            self._data.loc[:,"NEW"] = self._data[old_col].replace(replace_dict)
            if missing_na and any(~self._data["NEW"].isin(replace_dict.values())):
                self._data.loc[~self._data["NEW"].isin(replace_dict.values()), "NEW"] = np.nan

        ON_counts = self._data\
            .groupby(["NEW", old_col], as_index=False)\
            .size()\
            .rename(columns={"size":"counts"})

        log_str = '\n'.join(
            ['%d changes: "%s -> %s' % (C, O, N)
            for N, O, C in zip(ON_counts['NEW'], ON_counts[old_col], ON_counts['counts'])]
        )
        log_str = log_str + ("\n%d values in %s now NA"
                             % (self._data["NEW"].isnull().sum(), new_col))
        self.log.info(log_str)
        self._data.loc[:, new_col] = self._data["NEW"]
        del self._data["NEW"]
        return self

    def map(self, col, func):
        assert isinstance(col,str)

        if col in self._data.columns:
            log_str=''
            assert callable(func)
            nulls = self._data[col].isnull().sum()
            self._data[col] = self._data[col].map(func)
            log_str = log_str + \
                ("%d NA values before, now %d" % (nulls, self._data[col].isnull().sum()))
            self.log.info(("Executing: %s on %s\n" % (func, col)) + log_str)

        else:
            assert (not callable(func))
            self._data[col] = func
            self.log.info("Creating new column: '%s' with value: %s" % (col, func))

        return self

    def exec(self, exec_str):
        exec_str = exec_str.replace('_DATA_', 'self._data')
        self.log.info('Adding column by exec("%s")', exec_str)
        assert not any(cond in exec_str for cond in ['import', 'exec', 'eval'])
        exec(exec_str)
        return self

    def apply(self, out_cols, func, in_cols, opts_dict=None):
        opts_dict = {} if opts_dict is None else opts_dict
        self._data[out_cols] = func(self._data[in_cols], **opts_dict)
        return self

    def copy_col(self, col, new_col):
        assert isinstance(col,str)
        assert col in self._data.columns
        assert new_col not in self._data.columns

        self._data[new_col] = self._data[col]
        self.log.info("Values of %s copied to %s" % (col, new_col))
        return self

    def fillna(self, cols, val=0):
        log_str = ''
        if not isinstance(cols, list):
            cols = [cols]
        for col in cols:
            assert col in self.columns
            nulls = self._data[col].isnull().sum()
            self._data[col].fillna(val, inplace=True)
            assert self._data[col].isnull().any() == False
            log_str = log_str + ("%d NA values in %s filled with %s\n"
                                 % (nulls, col , str(val)))
        self.log.info(log_str[:-2])
        return self

    def generate_dummy_cols(self, col, prefix):
        log_str = 'Dummy cols generated:\n'

        for val in self._data[col].unique():
            dcol = prefix + val
            self._data.loc[:, dcol] = (self._data[col] == val).astype(int)
            log_str = log_str + "Name = %s, N = %d\n" % (dcol, self._data[dcol].sum())
        self.log.info(log_str[:-2])

        return self

    def clean(self, clean_args_dict={}, inplace=True):
        try:
            from clean_functions import clean_data
        except ModuleNotFoundError:
            raise ModuleNotFoundError("clean_funnctions not could not be imported")
        else:
            cleaned_data = clean_data(self._data, **clean_args_dict, log=self.log)
            if inplace:
                self._data = cleaned_data
                return self
            else:
                return cleaned_data

    def assign_unique_ids(self, new_uid, auid_args_dict={}, start_uid = 1, inplace=True, data=None):
        assert (not self._uid is None or
                ('id_cols' in auid_args_dict.keys() and not auid_args_dict['id_cols'] is None) or
                ('conflict_cols' in auid_args_dict.keys() and not auid_args_dict['conflict_cols'] is None)),\
            'Assigning unique ids requires either a uid, id_cols, or conflict_cols'
        try:
            from assign_unique_ids_functions import assign_unique_ids
        except ModuleNotFoundError:
            raise ModuleNotFoundError("assign_uique_ids_functions not could not be imported")
        else:
            if auid_args_dict == {}:
                auid_args_dict['id_cols'] = []

            if data is None:
                auid_data = assign_unique_ids(self._data, new_uid,
                                              **auid_args_dict, log=self.log)
            else:
                auid_data = assign_unique_ids(data, new_uid,
                                              **auid_args_dict, log=self.log)
            auid_data[new_uid] = auid_data[new_uid] - auid_data[new_uid].min() + start_uid
            if inplace:
                self._data = auid_data
                self._uid = new_uid
                return self
            else:
                return auid_data

    def reuid(self, new_uid, skip_query=None):
        if skip_query is None:
            return self.assign_unique_ids(new_uid, {'id_cols' : [self.uid]})
        else:
            stored_df = self.data
            auid_data = self.assign_unique_ids(new_uid, {'id_cols' : [self.uid]},
                                            inplace=False,
                                            data=self.qfilter(skip_query,
                                                         inplace=False))
            stored_df = stored_df.merge(
                auid_data[[new_uid, self.uid]].drop_duplicates(),
                on=self.uid, how='left')
            assert stored_df.shape[0] == self.data.shape[0], 'Wrong shape'
            assert (self.data.query(skip_query)[self.uid].nunique() ==
                    auid_data[self.uid].nunique()), 'missing uids'

            self._data = stored_df
            self._uid = new_uid
            return self


    def aggregate(self, aggregate_data_args, inplace=True):
        assert not self._uid is None, 'aggregate_data requires a uid'
        try:
            from assign_unique_ids_functions import aggregate_data
        except ModuleNotFoundError:
            raise ModuleNotFoundError("assign_uique_ids_functions not could not be imported")
        else:
            agg_data = aggregate_data(self._data, self._uid, **aggregate_data_args)
            self.log.info('Aggregated data size = %d' % agg_data.shape[0])
            if inplace:
                self._data = agg_data
                return self
            else:
                return agg_data

    def unique(self, cols=None, inplace=True):
        if cols is None:
            ud_data = self._data.drop_duplicates()
        elif isinstance(cols, str):
            ud_data = self._data[cols].unique()
        elif isinstance(cols, list):
            assert len(set(cols)) == len(cols)
            ud_data = self._data[cols].drop_duplicates()
        self.log.info('Unique data size = %d' % ud_data.shape[0])
        if inplace:
            self._data = ud_data
            return self
        else:
            return ud_data

    ###### TODO ######
    def keep_duplicates(self, cols):
        pass
    def remove_duplicates(self, cols):
        pass


    def summary(self, cols):
        pass
        return self._data[cols]


'''FUNCTIONS'''

def standardize_columns(col_names, file_path_key, permit_missing_cols=False):
    """Standardizes input col_names by using column_names.yaml file's
       specified column name changes, determined by file_path_key.
    Parameters
    ----------
    col_names : list
    file_path_key : str
        Key that specifies column_names.yaml file specific column name changes
    permit_missing_cols : bool
        Whether or not to allow any columns in the data to be skipped

    Returns
    -------
    standard_cols : list
    """
    import yaml
    column_names_path = 'hand/column_names.yaml'
    # Try to read the reference file for converting column names
    with open(column_names_path) as file:
        col_dict = yaml.load(file, Loader=yaml.FullLoader)

    # Ensure that file path key is in col dict keys
    assert file_path_key in col_dict.keys(),\
        ('{0} is the file path key, but it is not in col_dict kets: {1}'
         '').format(file_path_key, col_dict.keys())
    # Get file specific column name dictionary
    colname_dict = col_dict[file_path_key]
    standard_cols = []
    if permit_missing_cols:
        standard_cols = [colname_dict[col_name]
                         if col_name in colname_dict.keys()
                         else col_name
                         for col_name in col_names]
    else:
        standard_cols = [colname_dict[col_name] for col_name in col_names]
    # Return standardized columns
    return standard_cols

def get_basic_logger(outpath=None, script_path='nofile'):
    """Creates a default logging object

    Parameters
    ----------
    outpath : str

    Returns
    -------
    logger : logging object
    """
    import logging
    import sys
    from __main__ import __name__
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s[%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S')
    stream_out = logging.StreamHandler(sys.stdout)
    stream_out.setFormatter(formatter)
    logger.addHandler(stream_out)

    logfile = '{}{}.log'.format('' if outpath is None else outpath+'/',
                                ('.'.join(script_path.split('.')[:-1])
                                 if '.' in script_path else script_path))
    file_handler = logging.FileHandler(logfile, mode='w')
    logger.addHandler(file_handler)

    logger.info("running {}".format(script_path))
    return logger


def string_strip(string, no_sep=False):
    """Remove unnecessary characters from string

    Parameters
    ----------
    string : str
    no_sep : bool
        If True, remove all non alpha-numeric characters
        If False, remove periods, commas, and redundant whitespace

    Returns
    -------
        stripped_string : str

    Examples
    --------
    >>> string_strip("Mary-Ellen.", False)
    'Mary-Ellen'
    >>> string_strip("     SADOWSKY,  J.R", False)
    'SADOWSKY JR'
    >>> string_strip("KIM-TOY", True)
    'KIMTOY'
    >>> string_strip("LUQUE-.ROSALES", True)
    'LUQUEROSALES'
    """
    string = re.sub("\s+([=-])\s+", "-", string)
    if no_sep:
        stripped_string = re.sub(r'[^\w\s]', '', string).replace(" ", "")
    else:
        stripped_string = re.sub(r'\s\s+', ' ',
                                 re.sub(r'^[ \s]*|\.|\,|\s$', '', string))
    return ' '.join(stripped_string.split())


def collapse_data(full_df, temp_id='TempID'):
    """Collapses dataframe to unique values
       returning collapsed data and a dataframe with original index
       and corresponding collapsed index values stored for later expansion
    Parameters
    ----------
    full_df : pandas DataFrame
    temp_id : str
        Temporary ID for collapsed data index in stored df
    Returns
    -------
    collapsed_df : pandas DataFrame
    stored_df : pandas DataFrame
        Stores indexes from full_df
        and temp_id col corresponding indexes in collapsed_df
    """
    cols = full_df.columns.tolist()
    full_df.insert(0, 'Index', full_df.index)
    rows = full_df.shape[0]
    collapsed_df = (full_df[cols]
                    .drop_duplicates()
                    .copy()
                    .reset_index(drop=True))
    collapsed_df.insert(0, temp_id, collapsed_df.index)
    stored_df = full_df.merge(collapsed_df, on=cols, how='inner')
    stored_df.drop(cols, axis=1, inplace=True)
    assert stored_df.shape[0] == rows
    del full_df['Index']
    del collapsed_df[temp_id]
    return collapsed_df, stored_df


def expand_data(collapsed_df, stored_df, temp_id='TempID'):
    """Expandas collapsed dataframe based on index and stored_df
       returning full dataframe with same indicies as pre-collapsed
    Parameters
    ----------
    collapsed_df : pandas DataFrame
        Should be output [0] from collapse_data() after some function applied
    stored_df : pandas DataFrame
        Should be output [1] from collapse_data()
    temp_id : str
        Should be same as temp_id in collapsed_data

    Returns
    -------
    full_df : pandas DataFrame
        Indexes will be identical to full_df input in collapse_data()
    """
    collapsed_df.insert(0, temp_id, collapsed_df.index)
    stored_df = stored_df.merge(collapsed_df, on=temp_id, how='inner')
    del collapsed_df[temp_id]
    full_df = stored_df\
        .sort_values('Index')\
        .set_index('Index')\
        .drop(temp_id, axis=1)
    full_df.index.name = None
    return full_df


def remove_duplicates(dup_df, cols=[], unique=False):
    """Removes rows that are non-unique based on values in specified columns.
       Exact opposite of keep_duplicates().
    Parameters
    ----------
    dup_df : pandas DataFrame
    cols : list
        Column names in dup_df to determine unique-ness of row
        If no columns specified, assumes all columns in dup_df
    unique : bool
    Returns
    -------
    rd_df : pandas DataFrame
        Dataframe of rows that were unique (based on input cols) in dup_df
        Sorted by values of the input cols
    """
    if not cols:
        cols = dup_df.columns.tolist()
    if unique:
        dup_df = dup_df.drop_duplicates()
    rd_df = dup_df[~dup_df.duplicated(subset=cols, keep=False)]\
        .sort_values(cols)
    return rd_df


def keep_duplicates(dup_df, cols=[]):
    """Keeps rows that are non-unique based on values in specified columns.
       Exact opposite of remove_duplicates().
    Parameters
    ----------
    dup_df : pandas DataFrame
    cols : list
        Column names in dup_df to determine unique-ness of row
        If no columns specified, assumes all columns in dup_df

    Returns
    -------
    kd_df : pandas DataFrame
        Dataframe of rows that were not-unique (based on input cols) in dup_df
        Sorted by values of the input cols
    """
    if not cols:
        cols = dup_df.columns
    kd_df = dup_df[dup_df.duplicated(subset=cols, keep=False)]\
        .sort_values(cols)
    return kd_df


def keep_conflicts(dup_df, cols=[], all_dups=True):
    """Keeps rows that are duplicates in any identified column

    Parameters
    ----------
    dup_df : pandas DataFrame
    cols : list
        Column names in dup_df to determine unique-ness of row
        If no columns specified, assumes all columns in dup_df
    all_dups : bool
        If True (default), keep all rows identified by keep_duplicates
        If False, keep only rows that have duplicates in each identified column
    Returns
    -------
    conflicts_df : pandas DataFrame
    """
    if not cols:
        cols = dup_df.columns.tolist()

    kdf_list = [keep_duplicates(dup_df, col) for col in cols]
    conflicts_df = pd.concat(kdf_list, sort=False)

    if not all_dups:
        ind_list = [kdf.index.tolist() for kdf in kdf_list]
        keep_list = list(set(ind_list[0]).intersection(*ind_list))
        conflicts_df = conflicts_df.loc[keep_list]

    conflicts_df = conflicts_df.drop_duplicates()\
        .sort_index()

    return conflicts_df

# def union_group(df, gid, cols, sep = '__', starting_gid=1):
#     """Adds group id (gid) numbers to data based on multiple columns
#     By taking the union of rows which contain an overlapping
#     set of values in each columns as collecting duplicates of specified
#     columns does not include columns that may be missing values.
#     This is similar to creating a network where edges connect values between
#     columns in the same row and identifying rows by connected component.
#     (ex. looking at dates:
#      (A) 7/6/1993,  (B) 11/19/1930, and  (C) 7/21/1930
#      would be in the same group as (A) shares 7 as the month with (C)
#      and  (B) shares 1930 as the year with (C),
#      while (D) 6/20/1931 has a separate group since it has no element
#      in common with any of (A), (B), or (C), despite (A) having a 6 in it,
#      this is for the day field while (D) has 6 in the month field)

#     Parameters
#     ----------
#     df : pandas DataFrame
#     gid : str
#     cols : list
#         Column names to be used for identifying groups
#     sep : str
#         Separator for temporary values
#     starting_gid : int

#     Returns
#     -------
#     out_df : pandas DataFrame
#     """
#     import networkx as nx

#     all_vals = set()
#     temp_cols = []
#     # Null rows
#     df = df.copy()
#     df.insert(0, 'TEMPROWID', df.index)
#     null_df = df.loc[df[cols].isnull().all(axis=1)]\
#         .copy()\
#         .reset_index(drop=True)
#     df = df[df[cols].notnull().any(axis=1)]
#     # Create temporary columns to ensure no overlapping of values between columns
#     for col in cols:
#         df.loc[df[col].notnull(),'temp_'+col] =\
#             df.loc[df[col].notnull(), col].map(lambda x: col + sep + str(x))

#         assert not all_vals & set(df['temp_'+col].dropna())
#         all_vals.update(set(df['temp_'+col].dropna()))
#         temp_cols.append('temp_'+col)
#     # Generate edge list of connections between column values by row
#     el = []
#     for i,r in df[temp_cols].drop_duplicates().iterrows():
#         vals = r.dropna().tolist()
#         if len(vals) > 1:
#             els = list(itertools.combinations(vals,2))
#             el.extend(els)
#         else:
#             el.append((vals[0], vals[0]))
#     # Collect edgelist into list of connected components
#     cc = nx.connected_components(
#             nx.from_pandas_edgelist(
#                 pd.DataFrame(el, columns=['H', 'T']),
#                 'H','T'))
#     # Turn column values into list of 'nodes' with group ids
#     ccl = []
#     for group in cc:
#         ccl.extend(list(zip([starting_gid]*len(group), group)))
#         starting_gid+=1
#     node_df = pd.DataFrame(ccl, columns=[gid, 'node'])
#     out_df = pd.DataFrame()
#     # Iterate over temporary columns and merge back group ids using 'nodes'
#     for col in temp_cols:
#         mdf = df[['TEMPROWID', col]].merge(node_df, left_on=col,
#                                         right_on='node', how='inner')
#         out_df = out_df.append(mdf[['TEMPROWID', gid]], sort=False)
#         df.drop(col, axis=1, inplace=True)

#     out_df = df.merge(out_df.drop_duplicates(), on='TEMPROWID', how='left')
#     null_df[gid] = null_df.index + starting_gid
#     assert set(null_df.columns.tolist()) == set(out_df.columns.tolist())

#     out_df = out_df.append(null_df).set_index('TEMPROWID')
#     out_df.index.name = None

#     return out_df


def reshape_data(df, reshape_col, id_col):
    """Reshapes dataframe from wide to long for a single columns
       preservers observations with only NaN values in reshape columns

    Parameters
    ----------
    df : pandas DataFrame
    reshape_col : str
        Name of column to be reshaped by ('star' for star1, star2, ...)
    id_col : str
        Name of column containing unique ids

    Returns
    ----------
    long_df : pandas DataFrame
    """
    long_df = pd.wide_to_long(
        df, [reshape_col], j=reshape_col+'_num', i=id_col)\
        .dropna(subset=[reshape_col])\
        .reset_index()\
        .drop(reshape_col+'_num', axis=1)
    dropped_ids = list(set(df[id_col]) - set(long_df[id_col]))
    if dropped_ids:
        long_df = long_df.append(
            df.loc[df[id_col].isin(dropped_ids),
                   list_diff(long_df.columns, [reshape_col])],
                   sort=True)

    long_df = long_df.drop_duplicates().reset_index(drop=True)

    return long_df


def fill_data(df, id_col):
    """Creates dataframe of products of all non-nan value in fill_columns
    Parameters
    ----------
    df : pandas DataFrame

    Returns
    ----------
    filled_df : pandas DataFrame
    """
    cols = df.columns
    df = df[cols].drop_duplicates()
    rd_df = remove_duplicates(df, id_col)
    kd_list = []

    def df_product(df):
        """Iterates over columns and creates list of list of non-nan values"""
        non_nan_list = []
        for col in cols:
            non_nan_vals = df[col].dropna().unique().tolist()
            if non_nan_vals:
                non_nan_list.append(non_nan_vals)
            else:
                non_nan_list.append([np.nan])
        return list(itertools.product(*non_nan_list))

    for ind, grp in keep_duplicates(df, id_col).groupby(id_col):
        kd_list.extend(df_product(grp))

    filled_df = pd.DataFrame(kd_list, columns=cols)\
        .append(rd_df, sort=False)\
        .sort_values(id_col)\
        .reset_index(drop=True)
    return filled_df


def list_unique(dup_list):
    """Returns list of first unique values in a list
    Parameters
    ----------
    dup_list : list

    Returns
    -------
    unique_list : list

    Examples
    --------
    >>> list_unique([3,2,1,3,2,1,1,2,1,1])
    [3, 2, 1]
    >>> list_unique([])
    []
    """
    unique_list = []
    for i in dup_list:
        if i not in unique_list:
            unique_list.append(i)
    return unique_list


def list_intersect(list1, list2, unique=True):
    """Returns list of (unique) elements in list1 and list2 in order of list1
    Parameters
    ----------
    list1 : list
    list2 : list
    unique : bool
        If True (default) unique elements intersected elements are returned
        If False uniqueness is not enforced

    Returns
    -------
    intersected_list : list

    Examples
    --------
    >>> list_intersect(['A', 3, 3, 4, 'D'], ['D', 'B', 99, 3, 'A', 'A'], True)
    ['A', 3, 'D']
    >>> list_intersect([1,2,3], [4,5,6])
    []
    >>> list_intersect([1,2,3,1], [4,5,6,1], False)
    [1, 1]
    """
    if unique:
        list1 = list_unique(list1)
    intersected_list = [i for i in list1 if i in list2]
    return intersected_list


def list_diff(list1, list2, unique=True):
    """Returns list of (unique) elements in list1
       but not in list2 in order of list1
    Parameters
    ----------
    list1 : list
    list2 : list
    unique : bool
        If True (default) unique elements are returned
        If False uniqueness is not enforced

    Returns
    -------
    setdiff_list : list

    Examples
    --------
    >>> list_diff([1, 2, 2, 3, 1, 2, 3], [3, 2, 14, 5, 6])
    [1]
    >>> list_diff([1,1,2,3,4,2], [4,2,3], False)
    [1, 1]
    >>> list_diff([], [1,2,3])
    []
    """
    if unique:
        list1 = list_unique(list1)
    diff_list = [i for i in list1 if i not in list2]
    return diff_list


def list_union(list1, list2, unique=True):
    """Returns (unique) union of elements in list1 and list2
    Parameters
    ----------
    list1 : list
    list2 : list
    unique : bool
        If True (default) unique elements are returned
        If False uniqueness is not enforced

    Returns
    -------
    union_list : list

    Examples
    --------
    >>> list_union([1, 2, 2, 3, 4, 3], [6, 2, 3, 1, 9])
    [1, 2, 3, 4, 6, 9]
    >>> list_union([1, 2, 2, 3, 4, 3], [6, 2, 3, 1, 9], False)
    [1, 2, 2, 3, 4, 3, 6, 2, 3, 1, 9]
    """
    union_list = list(list1) + list(list2)
    if unique:
        union_list = list_unique(union_list)
    return union_list


if __name__ == "__main__":
    import doctest
    doctest.testmod()
    doctest.run_docstring_examples(list_unique, globals())
    doctest.run_docstring_examples(list_intersect, globals())
    doctest.run_docstring_examples(list_diff, globals())
    doctest.run_docstring_examples(list_union, globals())

#
# end
