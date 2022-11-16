#!usr/bin/env python3
#
# Author(s):    Roman Rivera

'''format script for arrest-info_2001-2017_2017-07'''

import __main__
import pandas as pd
import numpy as np
import os
import requests
from setup import do_setup
from general_utils import FormatData, string_strip

def get_setup():
    script_path = __main__.__file__
    args = {
        'input_files': [
            "input/12181-FOIA-P058944-Arrests2001-StecklowRubeinstein.xlsx",
            "input/12181-FOIA-P058944-Arrests2002-StecklowRubeinstein.xlsx",
            "input/12181-FOIA-P058944-Arrests2003-StecklowRubeinstein.xlsx",
            "input/12181-FOIA-P058944-Arrests2004-StecklowRubeinstein.xlsx",
            "input/12181-FOIA-P058944-Arrests2005-StecklowRubeinstein.xlsx",
            "input/12181-FOIA-P058944-Arrests2006-StecklowRubeinstein.xlsx",
            "input/12181-FOIA-P058944-Arrests2007-StecklowRubeinstein.xlsx",
            "input/12181-FOIA-P058944-Arrests2008-StecklowRubeinstein.xlsx",
            "input/12181-FOIA-P058944-Arrests2009-StecklowRubeinstein.xlsx",
            "input/2010 arrests export-rev.csv",
            "input/2011 arrests export.csv",
            "input/2012 arrests export.csv",
            "input/2013 arrests export.csv",
            "input/2014 arrests export.csv",
            "input/2015 arrests export.csv",
            "input/2016 arrests export.csv",
            "input/2017 arrests export.csv",
            ],
        'output_file': 'output/arrest-info_2001-2017_2017-07.csv.gz',
        'column_names_key': 'arrest-info_2001-2017_2017-07',
        }

    assert all(input_file.startswith('input/')
               for input_file in args['input_files']),\
        "An input_file is malformed: {}".format(args['input_files'])
    assert (args['output_file'].startswith('output/') and
            args['output_file'].endswith('.csv.gz')),\
        "output_file is malformed: {}".format(args['output_file'])

    return do_setup(script_path, args)


cons, log = get_setup()

# Write data into 9999 increments and create bash script
def curl_address(file_num):
    import os
    import datetime
    curl_string = ("curl --form addressFile=@output/addresses_%d.csv "
                  "--form benchmark=Public_AR_Current "
                  "https://geocoding.geo.census.gov/geocoder/locations/addressbatch "
                  "--output output/geocoderesult_%d.csv"
                  % (file_num,file_num))
    print(curl_string)
    start = datetime.datetime.now()
    os.system(curl_string)
    elasped = round((datetime.datetime.now()-start).total_seconds()/60)
    print("Completed Task#%d in %d minutes" % (file_num, elasped))
    return 0

def geocode_address(address):
  base_url = ("https://geocoding.geo.census.gov/geocoder/locations/address?"
              "street={}&city=Chicago&state=IL&"
              "benchmark=9&format=json")
  results = requests.get(url=base_url.format(address)).json()
  try:
    results = results['result']['addressMatches']
    if type(results)==list:
        results = results[0]['coordinates']
    else:
        results = results['coordinates']
    print("Success %s" % address)
    return results
  except Exception as e:
    print("Failed: %s" % address)
    print(e)
    return {'x' : np.nan, 'y' : np.nan}

def geocode_address_list(l):
    ID, ad1, ad2 = l
    r1 = geocode_address(ad1)
    if pd.isnull(r1['x']):
        r2 = geocode_address(ad2)
        r2['ID'] = ID
        r2['full_address'] = ad2
        return r2
    else:
        r1['ID'] = ID
        r1['full_address'] = ad1
        return r1

full_data = pd.DataFrame()
for in_file in cons.input_files:
    if in_file.startswith("input/12181"):
        year = int(in_file[len(in_file)-29:len(in_file)-25])
    else:
        year = int(in_file[6:10])

    if year in [2015, 2016, 2017]:
        skiprows=4
    elif year==2013:
        skiprows=6
    else:
        skiprows=5
    fd = FormatData(log=log)\
        .import_data(
            in_file, column_names=cons.column_names_key,
            skiprows=skiprows,
            encoding=("ISO-8859-1" if year>=2010 else None))\
        .copy_col('fbi_code', 'crime_code')\
        .add_columns([{'exec' : '_DATA_["first_name"] = _DATA_["first_name"].fillna("").str.replace("JR III", "JRIII")'}])\
        .clean()\
        .add_columns([{'exec' : '_DATA_["year"] = {}'.format(year)}])\
        .dropna(dna_args={'thresh' : 7})

    if year in [2015, 2017]:
        fd = fd.set_columns({'street_name' : 'street_number',
                        'street_number' : 'street_name'})

    full_data = full_data.append(fd.data)

log.info("Export data")
FormatData(full_data, log=log)\
    .map('cb_no', lambda x: pd.to_numeric(x, errors='coerce'))\
    .qfilter("cb_no==cb_no")\
    .write_data(cons.output_file)