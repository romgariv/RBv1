from collections import OrderedDict
BASE_OD = OrderedDict(
    [('star', ['star', '']),
     ('first_name', ['first_name_NS', 'F4FN']),
     ('last_name', ['last_name_NS', 'F4LN']),
     ('appointed_date', ['appointed_date']),
     ('birth_year', ['birth_year', 'current_age', '']),
     ('middle_initial', ['middle_initial', '']),
     ('middle_initial2', ['middle_initial2', '']),
     ('gender', ['gender', '']),
     ('race', ['race', '']),
     ('suffix_name', ['suffix_name', '']),
     ('current_unit', ['current_unit', ''])]
)
import itertools
import pandas as pd
import numpy as np
from general_utils import FormatData, list_diff, list_intersect,\
    keep_duplicates, keep_conflicts, get_basic_logger #, union_group, 


class MergeData:

    def __init__(self, ref, sup=None, ruid=None, suid=None, log=None):
        if log is None:
            self.log = get_basic_logger()
        else:
            self.log = log

        if isinstance(ref, pd.DataFrame):
            assert not ruid is None and ruid in ref.columns, 'Need a ruid'
            self.rdf = ref
            self.ruid = ruid
        else:
            assert not ref.uid is None, 'ref.uid cannot be None'
            self.rdf = ref.data
            self.ruid = ref.uid

        if not sup is None:
            if isinstance(sup, pd.DataFrame):
                assert not suid is None and suid in sup.columns, 'Need a suid'
                self.sdf = sup
                self.suid = suid
            else:
                assert not sup.uid is None, 'sup.uid cannot be None'
                self.sdf = sup.data
                self.suid = sup.uid

            assert self.ruid != self.suid, 'ref and sup uids cannot be the same'

            self.mdf = pd.DataFrame(columns=[self.ruid, self.suid, 'matched_on'])
            self.conflicts = pd.DataFrame(columns=[self.ruid, self.suid, 'matched_on'])

    @property
    def to_FormatData(self):
        return FormatData(self.rdf, uid=self.ruid, log=self.log)
    @property
    def rudf(self):
        return self.rdf[~self.rdf[self.ruid].isin(self.mdf[self.ruid])]

    @property
    def sudf(self):
        return self.sdf[~self.sdf[self.suid].isin(self.mdf[self.suid])]

    @property
    def rmdf(self):
        return self.rdf[self.rdf[self.ruid].isin(self.mdf[self.ruid])]

    @property
    def smdf(self):
        return self.sdf[self.sdf[self.suid].isin(self.mdf[self.suid])]

    @property
    def rcdf(self):
        return self.rdf[~self.rdf[self.ruid].isin(self.conflicts[self.ruid])]

    @property
    def scdf(self):
        return self.sdf[~self.sdf[self.suid].isin(self.conflicts[self.suid])]

    # @property
    # def conflict_groups(self):
    #     grouped_conflicts = self.conflicts
    #     # TODO FIX THIS??
    #     grouped_conflicts = union_group(grouped_conflicts, 'CGID', cols=[self.ruid, self.suid])
    #     return grouped_conflicts.sort_values('CGID')

    def print_intersect(self, cols, use_and = True, sort_by=None, select=None):
        if not isinstance(cols, list):
            cols = list(cols)
        if sort_by is None:
            sort_by = cols
        rudf = self.rudf
        sudf = self.sudf

        slocs = None
        rlocs = None
        for col in cols:
            reduce_func = pd.Series.__and__ if use_and else pd.Series.__or__
            intr = set(rudf[col]) & set(sudf[col]) - set(np.nan)
            if not (sr is None or rr is None):
                slocs = reduce_func(slocs, sudf[col].isin(intr))
                rlocs = reduce_func(rlocs, rudf[col].isin(intr))
            else:
                slocs = sudf[col].isin(intr)
                rlocs = rudf[col].isin(intr)
        print(rudf.loc[rlocs].sort_values(sort_by))
        print(sudf.loc[slocs].sort_values(sort_by))

    def add_sup_data(self, sup, suid=None):
        if isinstance(sup, pd.DataFrame):
            assert not suid is None and suid in sup.columns, 'Need a suid'
            self.sdf = sup
            self.suid = suid
        else:
            assert not sup.uid is None, 'sup.uid cannot be None'
            self.sdf = sup.data
            self.suid = sup.uid

        assert self.ruid != self.suid, 'ref and sup uids cannot be the same'

        self.mdf = pd.DataFrame(columns=[self.ruid, self.suid, 'matched_on'])
        self.conflicts = pd.DataFrame(columns=[self.ruid, self.suid, 'matched_on'])
        return self

    @staticmethod
    def generate_on_lists(data_cols,  base_OD, custom_merges, base_OD_edits):
        merge_lists = []
        base_OD_edits = (base_OD_edits
                         if isinstance(base_OD_edits, OrderedDict)
                         else OrderedDict(base_OD_edits))
        for k, v in base_OD_edits.items():
            base_OD[k] = v
        filtered_base_lists = [list_intersect(col_list, data_cols + [''])
                               for col_list in base_OD.values()]

        if filtered_base_lists == []:
            return custom_merges
        else:
            filtered_base_lists = [fbl for fbl in filtered_base_lists if fbl]
            on_lists = list(itertools.product(*filtered_base_lists))
            on_lists = [[i for i in ol if i]
                        for ol in on_lists]
            on_lists.extend(custom_merges)
            return on_lists

    def generate_merge_report(self, total_merged_pairs, total_mref, total_msup, total_ref, total_sup):
        unmerged_ref = total_ref - total_mref
        unmerged_sup = total_sup - total_msup
        prcnt_m_ref = round(100 * total_mref / total_ref, 2)
        prcnt_m_sup = round(100 * total_msup / total_sup, 2)
        prcnt_um_ref = round(100 * unmerged_ref / total_ref, 2)
        prcnt_um_sup = round(100 * unmerged_sup / total_sup, 2)
        merge_report = ('Merge Report:\n'
                        '{0} Total Merged Pairs: {1}% of ref and {2}% of sup Merged.'
                        '\n{3} Unmerged in ref. {4}% Unmerged.\n'
                        '{5} Unmerged in sup. {6}% Unmerged.'
                        '').format(total_merged_pairs,
                                   prcnt_m_ref, prcnt_m_sup,
                                   unmerged_ref, prcnt_um_ref,
                                   unmerged_sup, prcnt_um_sup)
        return merge_report

    def cycle_data(self, merge_cols, cycle, loc_merge_list, na_policy):
        rdft = self.rdf
        sdft = self.sdf
        if cycle in ['ref', 'both']:
            rdft = rdft[~rdft[self.ruid].isin(self.mdf[self.ruid])]
        if cycle in ['sup', 'both']:
            sdft = sdft[~sdft[self.suid].isin(self.mdf[self.suid])]


        if isinstance(merge_cols, dict):
            rdft = rdft.query(merge_cols['query'])
            sdft = sdft.query(merge_cols['query'])
            merge_cols = merge_cols['cols']

        if all(loc_merge_list):
            loc_cols, loc_start, loc_end, event_col = loc_merge_list
            rdft = rdft[[self.ruid] + merge_cols + loc_cols + [loc_start, loc_end]].dropna(how=na_policy)
            sdft = sdft[[self.suid] + merge_cols + loc_cols + [event_col]].dropna(how=na_policy)
            merge_cols = merge_cols + loc_cols
        else:
            rdft = rdft[[self.ruid] + merge_cols].dropna(how=na_policy)
            sdft = sdft[[self.suid] + merge_cols].dropna(how=na_policy)
        return (rdft, sdft, merge_cols)

    def safe_merge(self, merge_cols, na_policy,
                   loc_merge_list, mode, cycle):

        ruids = self.rdf[self.ruid].nunique()
        suids = self.sdf[self.suid].nunique()
        loc_merge = all(loc_merge_list)

        rdft, sdft, merge_cols = self.cycle_data(merge_cols, cycle, loc_merge_list, na_policy)

        nrtuids = rdft[self.ruid].nunique()
        nstuids = sdft[self.suid].nunique()
        assert len(set(merge_cols)) == len(merge_cols), 'Duplicate name columns'

        if rdft.empty or sdft.empty:
            mdft = pd.DataFrame(columns=[self.ruid, self.suid]+merge_cols)
        else:
            try:
                mdft = rdft.merge(sdft,how='inner', on=merge_cols)
            except ValueError:
                for mc in merge_cols:
                    dtypes = set((rdft[mc].dtype, sdft[mc].dtype))
                    if len(dtypes) == 2 and any('O' == dt for dt in dtypes):
                        self.log.warning("Converting dtype of %s to object" % mc)
                        rdft[mc] = rdft[mc].astype('object')
                        sdft[mc] = sdft[mc].astype('object')
                mdft = rdft.merge(sdft,how='inner', on=merge_cols)

        if loc_merge:
            loc_start, loc_end, event_col = loc_merge_list[1:]
            mdft = mdft[(mdft[event_col] >= mdft[loc_start]) &
                        (mdft[loc_end].isnull() | (mdft[event_col] <= mdft[loc_end]))]
        if mdft.empty:
            return mdft
        else:
            mdft = mdft[[self.ruid, self.suid]].drop_duplicates()
#             mc_str = ('\nMerging on: %s\n' % (merge_cols))
#             rm_str = ('%d (%.2f%%) %s uids dropped by dropna=%s\n'
#                       % (ruids - nrtuids, 100 * (ruids - nrtuids)/ruids, 'ref', na_policy))
#             sm_str = ('%d (%.2f%%) %s uids dropped by dropna=%s'
#                       % (suids - nstuids, 100 * (suids - nstuids)/suids, 'sup', na_policy))
#             self.log.info(mc_str + rm_str + sm_str)
            mdft = self.filter_conflicts(mdft, merge_cols, mode)
            return mdft

    def loop_merge(
            self, custom_merges=None, base_OD=BASE_OD, base_OD_edits=None,
            loc_cols=None, loc_start=None, loc_end=None, event_col=None,
            log_conflicts=True, verbose=True,
            na_policy='any', mode='oto', cycle='both'):

        intersect_cols = list_diff(list_intersect(self.rdf.columns, self.sdf.columns),
                                    [self.ruid, self.suid])
        on_lists = self.generate_on_lists(intersect_cols,
                                          OrderedDict(base_OD) if not base_OD is None else OrderedDict(),
                                          custom_merges if not custom_merges is None else [],
                                          base_OD_edits if not base_OD_edits is None else [])
        nruids = self.rdf[self.ruid].nunique()
        nsuids = self.sdf[self.suid].nunique()

        for merge_cols in on_lists:
            assert len(merge_cols) > 0
            mdft = self.safe_merge(merge_cols, na_policy,
                                   loc_merge_list = [loc_cols, loc_start, loc_end, event_col],
                                  mode=mode, cycle=cycle)
            if mdft.shape[0] > 0:
                if verbose:
                    print('%d Matches on \n %s columns' %(mdft.shape[0], merge_cols))
                if isinstance(merge_cols, dict):
                    mc_cols = merge_cols['cols'] + (loc_cols if not loc_cols is None else [])
                    mdft['matched_on'] = '%s : %s' % (merge_cols['query'], '-'.join(mc_cols))
                else:
                    mc_cols = merge_cols + (loc_cols if not loc_cols is None else [])
                    mdft['matched_on'] = '-'.join(mc_cols)
                self.mdf = self.mdf.append(mdft, sort=False)
            if self.sdf.shape[0] == 0:
                break
        self.mdf = self.mdf[[self.ruid, self.suid, 'matched_on']].reset_index(drop=True)
        self.evaluate_merge(nruids, nsuids, mode, log_conflicts)
        return self

    def filter_conflicts(self, mdft, merge_cols, mode):
        if mode == 'oto':
            conflicts = keep_conflicts(mdft, all_dups=True)
        elif mode == 'mto':
            conflicts = keep_duplicates(mdft, self.ruid)
        elif mode == 'otm':
            conflicts = keep_duplicates(mdft, self.suid)
        else:
            return mdft
        mdft = mdft.drop(conflicts.index)
        conflicts['matched_on'] = '-'.join(merge_cols)
        self.conflicts = self.conflicts.append(conflicts, sort=False)
        return mdft

#         if not conflicts.empty:
#             mdft = mdft.drop(conflicts.index)
#             rc_str = ('%d (%.2f%%) %s uids dropped due to conflicts'
#                       % (nrtuids - conflicts[self.ruid].nunique(),
#                          100 * (nrtuids - conflicts[self.ruid].nunique())/nrtuids, 'ref'))
#             sc_str = ('%d (%.2f%%) %s uids dropped due to conflicts'
#                       % (nstuids - conflicts[self.suid].nunique(),
#                          100 * (nstuids - conflicts[self.suid].nunique())/nstuids, 'sup'))
#             self.log.warning(rc_str + '\n' + sc_str)
#             self.conflicts.append(conflicts, sort=False)

    def evaluate_merge(self, nruids, nsuids, mode, log_conflicts):
        self.log.info(self.generate_merge_report(
            self.mdf.shape[0], self.mdf[self.ruid].nunique(), self.mdf[self.suid].nunique(),
            nruids, nsuids))
        self.log.info('\n%s', self.mdf['matched_on'].value_counts())
        if not self.conflicts.empty:
            if log_conflicts:
                warn_str = ('Conflicts occurred while merging \n Ref: %s \n Sup: %s'
                            % (self.rdf[self.rdf[self.ruid].isin(self.conflicts[self.ruid])].sort_values(self.ruid),
                            self.sdf[self.sdf[self.suid].isin(self.conflicts[self.suid])].sort_values(self.suid)))
                self.log.warning("Conflicts on: \n%s" % (self.conflicts))
                self.log.warning(warn_str)
            else:
                self.log.warning("Conflict sizes: %d ref, %d sup" % (
                    self.rdf[self.rdf[self.ruid].isin(self.conflicts[self.ruid])].shape[0],
                    self.sdf[self.sdf[self.suid].isin(self.conflicts[self.suid])].shape[0]))
        return None

    def append_to_reference(self, keep_um=True, drop_cols=None,
                            inplace=False, full_rdf=None):
        if full_rdf is None:
            full_rdf = self.rdf
        else:
            assert not (set(self.mdf[self.ruid]) - set(full_rdf[self.ruid]))

        full_sdf = self.sdf
        if drop_cols is None: drop_cols = []

        ref_link = pd.concat([
            self.mdf[[self.ruid, self.suid]],
            full_rdf.loc[~full_rdf[self.ruid].isin(self.mdf[self.ruid]),
                         [self.ruid]].drop_duplicates()],
                         sort=False)
        if keep_um and not self.sudf.empty:
            sup_link = self.sdf\
                .loc[~self.sdf[self.suid].isin(self.mdf[self.suid]),
                     [self.suid]]\
                .drop_duplicates()\
                .reset_index(drop=True)
            sup_link[self.ruid] = sup_link.index + max(ref_link[self.ruid]) + 1
            assert max(sup_link[self.ruid]) - min(ref_link[self.ruid]) + 1 == \
                (ref_link[self.ruid].nunique() + sup_link[self.suid].nunique()),\
                'Link DF + sup_link wrong size.'

            ref_link = ref_link.append(sup_link, sort=False)\
                .sort_values(self.ruid)\
                .reset_index(drop=True)

        full_sdf[self.suid] = full_sdf[self.suid].astype(float)
        ref_link[[self.ruid, self.suid]] = ref_link[[self.ruid, self.suid]]\
            .astype(float, errors='raise')
        full_sdf = full_sdf.merge(ref_link[[self.ruid, self.suid]], on=self.suid, how='left')
        full_rdf = full_rdf\
            .append(full_sdf, sort=False)\
            .dropna(subset=[self.ruid], axis=0, how='any')\
            .drop(drop_cols, axis=1)\
            .drop_duplicates()\
            .sort_values(self.ruid)\
            .reset_index(drop=True)

        assert max(full_rdf[self.ruid]) - min(full_rdf[self.ruid]) + 1 == full_rdf[self.ruid].nunique(),\
            'Missing some uids'
        full_rdf[[self.ruid, self.suid]] = full_rdf[[self.ruid, self.suid]]\
            .astype(float, errors='raise')
        full_rdf[self.ruid] = full_rdf[self.ruid].astype(int, errors='raise')
        if inplace:
            self.rdf = full_rdf
            return self
        else:
            return FormatData(full_rdf, uid=self.ruid, log=self.log)

    def remerge(self, main_sup):
        if isinstance(main_sup, str):
            if os.path.isfile(main_sup):
                main_sup = pd.read_csv(main_sup)
        rows = main_sup.shape[0]
        link_df = self.rdf[[self.ruid, self.suid]].drop_duplicates()
        main_sup = main_sup.merge(
            link_df, on=self.suid, how='left')
        main_sup[[self.ruid, self.suid]] = main_sup[[self.ruid, self.suid]].apply(pd.to_numeric)
        assert main_sup.shape[0] == rows, 'Missing rows!'
        return main_sup


    def remerge_to_file(self, input_path, output_path,
                        csv_opts={'index' : False, 'compression' : 'gzip'}):
        """Merges sup_df (with uids) to input_path, writes data to output_path

        Parameters
        ----------
        input_path : str
            Path of input file (csv)
        output_path : str
            Path of output file (csv)
        csv_opts : dict
            Dictionary of pandas .to_csv options

        Returns
        ----------
        self
        """
        full_df = pd.read_csv(input_path)
        rows = full_df.shape[0]
        link_df = self.rdf[[self.ruid, self.suid]].drop_duplicates()
        full_df = full_df.merge(
            link_df, on=self.suid, how='left')
        assert full_df.shape[0] == rows, 'Missing rows!'
        full_df.to_csv(output_path, **csv_opts)
        return self

# Done
