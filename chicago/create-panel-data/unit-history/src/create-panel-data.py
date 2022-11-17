#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''create-master-data script for unit-history'''

import __main__
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime

from setup import do_setup
from general_utils import FormatData, remove_duplicates, keep_duplicates
from unit_history_functions import resolve_units, history_to_panel

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/unit-history__2016-12.csv.gz',
        'output_monthly_file' : 'output/monthly-panel_unit-history_%s.csv.gz',
        'output_daily_file' : 'output/daily-panel-%d_unit-history_1965-2016.csv.gz',
        'resolved_uh_file' : 'output/unit-history_resolved.csv.gz',
        'first_unit_file' : 'output/first-geo-unit.csv.gz',
        'UID': 'NUID',
        'START' : 'unit_start_date',
        'END' : 'unit_end_date',
        'UNIT' : 'unit',
    }

    assert args['input_file'].startswith('input/'),\
        "Input_files are malformed: {}".format(args['input_file'])
    assert (args['output_monthly_file'].startswith('output/') and
            args['output_monthly_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_monthly_file'])

    return do_setup(script_path, args)



cons, log = get_setup()

'''
EXPLANATION
1. For most recent unit history file, drop rows with missing unit, UID, or unit start info
2. Drop rows with non-null end dates that are before the start date
3. Select duplicates on UID, start, and unit columns, take maximum end date
    (note that any non-null end will override a null)
4. Use resolve_units function by UID which behaves as follows:
    The main point is to iterate over the unit history rows,
    determine which rows should have a higher priority than others (those with
    real end dates are high priority and should not have their starts/ends changed
    unless due to overlap with another high priority row). Rows with low priority
    (no real end dates) are flexible. Since low priority rows have no definitive ends,
    and if they overlap with high priority rows, the low priority row is duplicated,
    placed after the high priority row ends, and has its priority reduced by one.
    This avoids any gaps that may be introduced by favoring definitive end date (high
    priority) rows over missing end date (low priority) rows.
    1. Sorts unit history by starts (1st) and ends (2nd), and unit number (3rd)
        Meaning that earlier units come first with shorter time spans,
        and if start and end are the same and codes are the same,
        then lower units will be selected.
    2. Fills all null-ends with current date, but mark missing end dates as CODE = 0
       (higher code means higher priority, any CODE < 1 means a null-end date)
    3. Loops over all rows from earliest to latest
        Determines if there is an overlap between the current row and the next one
        IF there are overlaps between current row and next row:
                IF there is only one low priority row:
                    IF the low priority row starts before the higher priority:
                        Create a new row with the lower priority row,
                        Set the new row's start date as the day after the high priority row ends
                        And set the new row's Code to the low priority code - 1 (even lower priority)
                        Then set the low priority row's end date to be 1 day before the high row's start
                        Append the new row to the data
                    ELSE (low row starts after high row):
                        Set the low row's start t be 1 day after high row's end
                        And reduce low row's code by 1
                ELSE IF there are no low priority rows:
                    IF all end dates are the same:
                        IF there are multiple end dates:
                            Set the first (earliest end date) row's end to being 1 day before
                            the next row starts
                        ELSE (only one end date and one start date):
                            Delete the second row (perfect overlaps, takes row with smaller unit)
                    ELSE (two different end dates, possibly same start date, but it is arbitary):
                        Create a new row equal to the firsr row
                        Set the new row's start to 1 day after the second row's end
                        Set the new row's code to 0
                        Set the first row's end date to be 1 day before the second row's start date
                        Append the new row to the data
                ELSE IF there are two low priority rows:
                    IF there is 1 unique start date:
                        IF there is more than 1 unique CODE:
                            drop the row with the lowest priority
                        ELSE (same code level for both):
                            drop row with higher unit number
                    ELSE IF there is only 1 unique unit:
                        drop the second row (later start)
                    ELSE (multiple starts and multiple units):
                        set the first row's end to one day before the second row's start
        ELSE (not overlaps):
            Continue on to examine next row (and next next row)
5. Using the resolved unit history, create daily and monthly panel:
    1. Iterate over 50 roughly even groupings of NUIDs to avoid memory issues
    2. Create temporary resolved_group dataframe from resolved unit histories with only the selected NUIDs
    3. Generate daily panel data using history_to_panel with end dates after 1965 (so if someone starts/ends a unit before 1965, it is not recorded), but if someone starts a unit before 1965 and ends it after 1965, it is kept
    4. Once daily panel is generated, write it to its daily-panel-NUMBER file
    5. Begin generating monthly panel data by taking most common unit in a month
        1. Change day column to month column (replace all Y-m-d ds to 01 and change name to month)
        2. Generate 'rank' column indicating which observations occurred in what order for each NUID/month
        3. Create a duration ('size') panel (pg_size) which records the number of days a NUID was in a UNIT by month
        4. Take unique (no duplicate) UID x month x size combinations and take the row (UNIT in the NUID x month) with the largest 'size' (i.e. the most common unit)
        5. Keep duplicates of UID x month x size as some months may be split evenly between two units as pg_size
        6. If this is the case, merge the duplicated monthly size panel (pg_size) to the initial panels (left join to preserve order? then drop any NAs, i.e. those not in duplicated pg_size)
           to identify the ranking (which unit came last in the month), sort this by size then 'rank'
        7. Take the last row in each NUID x month combination, thus taking observation with the largest size and largest rank (no ranks can be duplicates), then drop the 'rank' column
        8. Append this resolved data to the previously generated panel_month
        9. If there are duplicates take the one with the largest size
           i.e. a unit history with 1 day in unit A, 28 in unit B, and 1 in unit C
                both unit B and unit C will appear as B is the 'largest'
                and C is a duplicate on size (with A) and the last in rank,
                then take the one with the largest size (i.e. B)
        10. From there ensure there are no duplicates and it is the correct size, then append the monthly data to the stored monthly_panel that will contain monthly panel data for all NUIDs (this is done 50 times due to daily panel data being broken into 50 pieces)
6. Using the appended monthly_panel data, compute unit durations
    NOTE: If an officer is in unit A, then next month he goes from A to B to A (A is most common), the data will not know he ever left, and his unit tenure will remain unbroken
7. Write complete monthly panel to file
'''


START = cons.START
END = cons.END
UNIT = cons.UNIT
UID = cons.UID


log.info("Getting unit history data from file: %s" % cons.input_file)
uh_df = pd.read_csv(cons.input_file)
uh_df = uh_df.loc[:, [UID, UNIT, START, END]]
uh_df.dropna(subset=[UNIT, UID, START],
             how='any', inplace=True)
uh_df[START] = pd.to_datetime(uh_df[START])
uh_df[END] = pd.to_datetime(uh_df[END])
uh_df = uh_df[uh_df[END].isnull() |
              (uh_df[START] < uh_df[END])]
uh_df = uh_df.drop_duplicates()
nUIDs = uh_df[UID].nunique()
uh_df = \
    keep_duplicates(uh_df, [UID, START, UNIT])\
        .groupby([UID, START, UNIT], as_index=False)\
        .max()\
        .append(remove_duplicates(uh_df, [UID, START, UNIT]), sort=False)

if os.path.exists(cons.resolved_uh_file):
    log.info("Resolved units file (%s) already exists. Will not re-resolve." % cons.resolved_uh_file)
    resolved = pd.read_csv(cons.resolved_uh_file)
else:
    log.info("Starting resolve_units")
    start_time = datetime.now()
    resolved = pd.concat(
        [resolve_units(g, START, END, UNIT)
         for k, g in uh_df[[UID, UNIT, START, END]].groupby(UID, as_index=False)], sort=False)
    log.info("Done resolving_units\nTook : %s\nwith CODE distribution:\n%s\nCompared to initial distribution:\n%s"
             % (datetime.now() - start_time,
                resolved.CODE.value_counts(),
                uh_df.unit_end_date.notnull().astype(int).value_counts()))

    resolved[START] = pd.to_datetime(resolved[START])
    resolved[END] = pd.to_datetime(resolved[END])
    assert resolved[START].notnull().all() and resolved[END].notnull().all()
    assert pd.to_datetime(resolved[START].max()) <= pd.to_datetime('2016-12-31') and pd.notnull(resolved[START].max())
    resolved.loc[resolved[END] > pd.to_datetime('2016-12-31'), END] = pd.to_datetime('2016-12-31')

    assert resolved[UID].nunique() == nUIDs, 'Lost UIDs during resolve_units'
    log.info('Dropping starts after 2016 and ends before 1965')
    resolved = resolved[(resolved[END] >= pd.to_datetime('1965-01-01')) &
                        (resolved[START] <= pd.to_datetime('2016-12-31'))]
    log.info("Writing resolved units to %s" % cons.resolved_uh_file)
    resolved.to_csv(cons.resolved_uh_file, **cons.csv_opts)


del uh_df


def make_durations(col):
    durs = []
    dur = 1
    for i in range(len(col)):
        if i == 0 or col.iloc[i-1] != col.iloc[i]:
            dur = 1
        else:
            dur += 1
        durs.append(dur)
    return pd.Series(durs)

ruids = resolved[UID].unique()
N = 50.0
log.info("Doing history to panel for %d groups of NUIDs" % N)
avg = len(ruids)/N
last = 0.0
i = 1
monthly_panel = pd.DataFrame()

while last < len(ruids):
    log.info("Beginning panel creation at %d", last)
    start_time = datetime.now()
    resolved_group = resolved[resolved[UID].isin(ruids[int(last):int(last+avg)])]
    panel = history_to_panel(
        resolved_group, 'day',
        max_date='2016-12-31',
        min_date='',
        log=log,  unit=UNIT, uid=UID,
        start=START, end=END,
        allow_dups=False)

    # log.info('Adding unit_durations... this also may take a while')
    # panel['unit_duration'] = panel\
    #     .groupby(UID, as_index=False)[UNIT]\
    #     .transform(make_durations)
    # log.info('Adding unit_durations... Done.')
    assert panel['day'].notnull().all()
    log.info("Writing panel #%d with %d UIDs (%d to %d)" % (i, panel[UID].nunique(), panel[UID].min(), panel[UID].max()))
    panel.to_csv((cons.output_daily_file % i), **cons.csv_opts)

    log.info("Aggregating by most common unit in month")
    panel.drop('event', axis=1, inplace=True)
    panel['day'] = panel['day'].astype(str).str.slice(0, -2) + '01'
    panel.rename(columns={'day' : 'month'}, inplace=True)
    panel['rank'] = panel.groupby([UID, 'month']).cumcount()+1
    pg_size = panel.groupby([UID, UNIT, 'month'], as_index=False)\
        .size()\
        .reset_index()\
        .rename(columns={0 : 'size'})

    panel_month = remove_duplicates(pg_size, [UID, 'month', 'size'])\
        .sort_values([UID, 'month', 'size'])\
        .groupby([UID, 'month'], as_index=False)\
        .last()
    pg_size = keep_duplicates(pg_size, [UID, 'month', 'size'])
    if not pg_size.empty:
        log.info("Some UID/UNIT/month combos with same size. %d rows" % pg_size.shape[0])
        pg_last = panel\
            .merge(pg_size, on=[UID, 'month', UNIT], how='left')\
            .dropna(how='any')\
            .sort_values([UID, 'month', 'size', 'rank'])\
            .groupby([UID, 'month'], as_index=False)\
            .last()\
            .drop('rank', axis=1)
        intended_rows = pg_size[[UID, 'month']].drop_duplicates().shape[0]
        assert pg_last.shape[0] == intended_rows

        panel_month = pg_last.append(panel_month, sort=False)

        assert keep_duplicates(panel_month, [UID, 'month', 'size']).empty
        if not keep_duplicates(panel_month, [UID, 'month']).empty:
            log.info("Some cases of last unit being short in month:\n%s\nTaking most common over last."
                     % (keep_duplicates(panel_month, [UID, 'month'])))
            panel_month = panel_month\
                .sort_values([UID, 'month', 'size'])\
                .groupby([UID, 'month'], as_index=False)\
                .last()
        assert keep_duplicates(panel_month, [UID, 'month']).empty


    assert panel_month.shape[0] == panel[[UID, 'month']].drop_duplicates().shape[0]
    monthly_panel = monthly_panel.append(panel_month, sort=False)
    log.info("Done aggregating to months, monthly panels appended")

    del intended_rows
    del pg_size
    del pg_last
    del panel
    del panel_month

    log.info("Finished panel creation at %d. Took: %s" % (last, datetime.now() - start_time))

    time.sleep(5)
    last += avg
    i+=1


log.info('Adding unit_durations... this also may take a while')
monthly_panel.sort_values([UID, 'month'], inplace=True)
monthly_panel['unit_duration'] = monthly_panel\
    .groupby(UID, as_index=False)[UNIT]\
    .transform(make_durations)
log.info('Adding unit_durations... Done.')
monthly_panel.rename(columns={'event' : 'unit_event'}, inplace=True)
assert monthly_panel['month'].notnull().all()
monthly_panel['month'] = monthly_panel['month'].astype(str)

mp = monthly_panel.query('month >= "2002-01-01"')
FormatData(mp, log=log).write_data(cons.output_monthly_file % '2002-2016')

mp = monthly_panel.query('month < "2002-01-01" & month >= "1985-01-01"')
FormatData(mp, log=log).write_data(cons.output_monthly_file % '1985-2001')

mp = monthly_panel.query('month < "1985-01-01"')
FormatData(mp, log=log).write_data(cons.output_monthly_file % '1965-1984')

# del mp, monthly_panel
# log.info("Getting first unit of each officer")
# first_units = pd.DataFrame()
# for f in os.listdir("output/"):
#     if not f.startswith("daily-panel"):
#         continue
#     first_units = first_units.append(
#         pd.read_csv('output/'+f)
#         .drop("event", axis=1)
#         .sort_values(["NUID", "day"])
#         .query("unit <= 25")
#         .groupby("NUID", as_index=False)
#         .first())
# first_units = first_units.sort_values("NUID")
# FormatData(first_units, log=log).write_data(cons.first_unit_file)
