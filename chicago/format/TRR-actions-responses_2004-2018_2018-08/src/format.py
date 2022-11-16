#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for TRR-actions-responses_2004-2018_2018-08'''

import __main__
import pandas as pd
import numpy as np
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/P456008 13094-FOIA-P456008-TRRdata Responsive Record Produced By R&A.xlsx',
        'output_file' : 'output/TRR-actions-responses_2004-2018_2018-08.csv.gz',
        'output_subject_file' : 'output/TRR-subject-actions_2004-2018_2018-08.csv.gz',
        'output_member_file' : 'output/TRR-member-actions_2004-2018_2018-08.csv.gz',
        'force_2018_file' : 'hand/trr_force_2018.csv',
        'unclassified_file' : 'hand/trr_unclassified_actions_2018.csv',
        'sheet' : 'ActionsResponses',
        'column_names_key': 'TRR-actions-responses_2004-2018_2018-08'
    }

    assert args['input_file'].startswith('input/'),\
        "Input_file is malformed: {}".format(args['input_file'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)



cons, log = get_setup()

fd = \
FormatData(log=log)\
    .import_data(cons.input_file, sheets=cons.sheet,
                 column_names=cons.column_names_key)\
    .clean()\
    .write_data(cons.output_file)\
    .qfilter("person == person")


assert fd.data.person.notnull().all()
assert set(fd.data.person.unique()) == set(("Member Action", "Subject Action"))

resistance_dict = {
    "Passive Resister" : "passive",
    "Active Resister" : "active",
    "Assailant Battery" : "battery",
    "Assailant Assault" : "assault",
    "Assailant Assault/Battery" : "assault",
    "Assailant Deadly Force" : "deadly"
}

resistance_level_dict = {
    "passive" : 0,
    "active" : 1,
    "battery" : 2,
    "assault" : 3,
    "assault" : 3,
    "deadly" : 4
}

sfd = FormatData(fd.data, log=log)\
    .qfilter('person == "Subject Action"')\
    .recode("resistance_type", "resistance_type", resistance_dict)\
    .recode("resistance_type", "resistance_level", resistance_level_dict)\
    .generate_dummy_cols("resistance_type", "resistance_")\
    .write_data(cons.output_subject_file)

log.info("Classifying member actions")

force_2018 = FormatData(log=log)\
    .import_data(cons.force_2018_file, add_row_id=False)\
    .data

force_2018_dict = dict(zip(force_2018['type_of_force'], force_2018['force_level']))

type_of_force_dict = {
    1 : ["MEMBER PRESENCE", "VERBAL COMMANDS"],
    2 : ["ESCORT HOLDS", "ARMBAR", "PRESSURE SENSITIVE AREAS","WRISTLOCK","CONTROL INSTRUMENT","O.C./CHEMICAL WEAPON", "O.C./CHEMICAL WEAPON W/AUTHORIZATION"],
    3 : ["TAKE DOWN/EMERGENCY HANDCUFFING", "OPEN HAND STRIKE", "CANINE"],
    4 : ["TASER (LASER TARGETED)", "TASER (PROBE DISCHARGE)","TASER (CONTACT STUN)", "TASER (SPARK DISPLAYED)"],
    5 : ["CLOSED HAND STRIKE/PUNCH","PUNCH","KICKS","KNEE STRIKE","ELBOW STRIKE","IMPACT WEAPON (DESCRIBE IN ADDITIONAL INFO)", "IMPACT MUNITION (DESCRIBE IN ADDITIONAL INFO)"],
    6 : ["FIREARM"]
}

mfd = FormatData(fd.data, log=log)\
    .qfilter('person == "Member Action"')\
    .recode("action","force_level_6", type_of_force_dict)\
    .recode("action", "force_level_28", force_2018_dict, missing_na=False)\
    .data

log.info("Recoding missing/Other actions using other_description")

string_dict = {
    6 : ["FIREARM", "SEMI AUTO", "CALIBER", "GUN",
         "FIRE", "REVOLVER", "RIFLE", "PISTOL"],
    5 : ["CLOSED HAND" , "FIST", "PUNCH", "KNEE", "BATON", "IMPACT MUNITION",
         "ELBOW", "KICK", "IMPACT", "ASSAILANT", "(?<!\\w)ASP(?!A-Z)"],
    4 : ["TA[S|Z]E[R|D]"],
    3 : ["PRESS?URE", "TAKE.?DOWN", "OPEN.?HAND", "OPENED HAND", "PALM", "CANINE",
         "OPEN BACKHAND", "EMERGENCY [HANDCUFFING|CUFFING|TAK|HANDCUFF]",
         "PHYSICALLY TOOK SUBJECT DOWN", "STUN", "CHEMICAL", "O\\.?C", "K.?9"],
    2 : ["LRAD", "ESCORT HOLD", "ARMBAR", "ARM BAR",
         "WRISTLOCK", "CONTROL INSTRUMENT", "TAKE DWN"],
}

for i in range(2,7):
    regex_str = '|'.join(string_dict[i])
    locs = (((mfd['action']=='OTHER (SPECIFY)') |
            (mfd['action'].isnull())) &
            mfd["force_level_6"].isnull())
    log.info("Recoding %d of %d rows to level %d with strings that contain: %s"
             % ((locs & mfd['other_description'].str.contains(regex_str)).sum(),
                locs.sum(), i, regex_str))
    mfd.loc[locs & mfd['other_description'].str.contains(regex_str),
            'force_level_6'] = i

force_level_dict = {
    6 : 'firearm',
    5 : 'major',
    4 : 'taser',
    3 : 'intermediate',
    2 : 'minor',
    1: 'none',
    0 : 'missing'
}

mfd = FormatData(mfd, log=log)\
    .fillna('force_level_6', 0)\
    .recode('force_level_6', 'type_of_force_6', force_level_dict)\
    .data
##

log.info("Recoding Other actions using other_description")

string_dict = {
    28 : ["FIREARM", "SEMI AUTO", "SEMI-AUTO", "CALIBER", "GUN",
         "FIRE", "REVOLVER", "RIFLE", "PISTOL", "SHOTGUN"],
    27 : ["IMPACT MUNI"],
    26 : ['BATON', 'IMPACT WEAP', "(?<!\\w)ASP(?!A-Z)"],
    25 : ["K.?9", "CANINE"],
    24 : ["TA[S|Z]E[R|D]"],
    23 : ["LRAD"],
    21 : ["CHEMICAL", "O\\.?C"],
    20 : ['KICK'],
    19 : ["KNEE"],
    18 : ["CLOSED HAND" , "FIST", "PUNCH", "STUN"],
    17 : ['ELBOW'],
    16 : ["TAKE.?DOWN", "EMERGENCY TAK", "TAKE DWN", "TOOK*.?DOWN"],
    15 : ["OPEN.?HAND", "OPENED HAND", "PALM", "OPEN BACKHAND"],
    14 : ['CUFF'],
    13 : ["PRESS?URE"],
    12 : ['CONTROL INS', 'INSTRU'],
    11 : ['ARMBAR', 'ARM BAR'],
    10 : ['WRIST'],
    9 : ['ESCORT HOLD', 'ESCORT'],
    8 : ['NONE'],
    6 : ['TACT.*?POSI'],
    4 : ['AVOIDED|MOVE.*?AVO'],
    2 : ['VERBAL', 'COMMAND', 'VERB.*?COMM'],
    1 : ['PRESS?ENCE'],
    0 : ['OTHER DESCRIPTION']
}

mfd.loc[mfd.other_description.isnull() &
        (mfd.force_level_28=="OTHER (SPECIFY)"), 'force_level_28'] = 0
log.info("%d null rows of other recoded to 0" % mfd.query("force_level_28==0").shape[0])

log.info("Break data into two categories: action in force_2018, and action='OTHER (SPECIFY)''")
mfd_rows = mfd.shape[0]

mfd['other_specify'] = (mfd['action'] == "OTHER (SPECIFY)").astype(int)
mfd_other = mfd[mfd.other_specify==1].copy()
mfd_known = mfd[mfd.other_specify!=1].copy()

assert len(set(mfd_known.ROWID) | set(mfd_other.ROWID)) == mfd_rows
assert mfd_known.shape[0] + mfd_other.shape[0] == mfd_rows

log.info("Cleaning OTHER (SPECIFY) force types")
log.info("Imputing actions where possible")
tua = FormatData(log=log)\
    .import_data(cons.unclassified_file, add_row_id=False)\
    .map('action', lambda x: x.upper())\
    .qfilter("action != 'OTHER DESCRIPTION'")\
    .set_columns({'action' : 'action_imputed'})\
    .recode("action_imputed", "force_level_28", force_2018_dict, missing_na=True)\
    .copy_col("force_level_28", "imputed_action")\
    .map("imputed_action", lambda x: 0 if pd.isnull(x) else 1)\
    .data

tua["filled_desciption"] = tua['action_imputed']

mfd_imputed = mfd_other\
    .drop("force_level_28", axis=1)\
    .merge(tua[['other_description', 'action_imputed', 'force_level_28', 'imputed_action']],
            on='other_description', how='inner')
mfd_other = mfd_other[~mfd_other.ROWID.isin(mfd_imputed.ROWID)]
mfd_other['imputed_action'] = 0

mfd_imputed['regex_action'] = 0
mfd_other['regex_action'] = 0

for i in sorted(string_dict.keys(), reverse=True):
    regex_str = '|'.join(string_dict[i])
    locs_imputed = (mfd_imputed.force_level_28.isnull() & mfd_imputed.action_imputed.str.contains(regex_str, regex=True))
    mfd_imputed.loc[locs_imputed, "force_level_28"] = i
    mfd_imputed.loc[locs_imputed, "regex_action"] = 1
    log.info("Recoding %d rows in imputed data to level %d with strings that contain: %s" % (locs_imputed.sum(), i, regex_str))

    locs_other = ((mfd_other.force_level_28=="OTHER (SPECIFY)") & mfd_other.other_description.str.contains(regex_str, regex=True))
    mfd_other.loc[locs_other, 'force_level_28'] = i
    mfd_other.loc[locs_other, "regex_action"] = 1
    log.info("Recoding %d rows in non-imputed data to level %d with strings that contain: %s" % (locs_other.sum(), i, regex_str))


mfd_known['imputed_action'] = 0
mfd_known['regex_action'] = 0

mfd = mfd_known.append(mfd_other).append(mfd_imputed).sort_values('ROWID')
mfd.loc[mfd.force_level_28=="OTHER (SPECIFY)", 'force_level_28'] = 0
assert mfd[(mfd.force_level_28==0) & (mfd.action!="OTHER (SPECIFY)")].empty
assert mfd.shape[0] == mfd_rows
mfd['force_level_28'] = pd.to_numeric(mfd['force_level_28'], errors='coerce')
assert mfd.force_level_28.notnull().all()

mfd = mfd.merge(force_2018.rename(columns={'force_level' : 'force_level_28',
                                           'type_of_force' : 'type_of_force_28'}),
                on='force_level_28', how='left')

mfd = FormatData(mfd, log=log)\
    .generate_dummy_cols('response_type_dtll', 'poaction_')\
    .write_data(cons.output_member_file)
