#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for TRR-actions-responses_2004-2016_2016-09'''
import __main__
from setup import do_setup
from general_utils import FormatData

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_file' : 'input/10655-FOIA-P046360-TRRdata_sterilized.xlsx',
        'output_file' : 'output/TRR-actions-responses_2004-2016_2016-09.csv.gz',
        'output_subject_file' : 'output/TRR-subject-actions_2004-2016_2016-09.csv.gz',
        'output_member_file' : 'output/TRR-member-actions_2004-2016_2016-09.csv.gz',
        'sheet' : 'ActionsResponses',
        'column_names_key': 'TRR-actions-responses_2004-2016_2016-09'
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
    .recode("action","type_of_force", type_of_force_dict)\
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
            mfd["type_of_force"].isnull())
    log.info("Recoding %d rows to level %d with strings that contain: %s"
             % (locs.sum(), i, regex_str))
    mfd.loc[locs & mfd['other_description'].str.contains(regex_str),
            'type_of_force'] = i

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
    .fillna('type_of_force', 0)\
    .recode('type_of_force', 'force_level', force_level_dict)\
    .generate_dummy_cols('force_level', 'poaction_')\
    .write_data(cons.output_member_file)
