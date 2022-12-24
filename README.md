# RBv1
Replication repository for CPD data projects beginning before 2020

Run on Python 3.7.6 . Install required modules in requirements.txt prior to replication.

Workflow is highly similar to that of https://github.com/invinst/chicago-police-data but does not rely on makefiles and has updated functions.

`share/` subdirectories: `src/` contains python files with functions used in data processing ; `hand/` contains data files which are used for data processing (e.g., `hand/gender_types.yaml` provides mappings from common gender codes in the raw data to standardized values) ; `tests` contains unit testing for `src/` files. `src/` files are passed to directories where they are used through symlinks (yes this is not efficient but it is how it was written). 

`chicago/` directories contains data and data cleaning/merge/formatting subdirectories. All `output/` folders contain relevant `.log` files which document what occurred in the file for reference.

  - `raw_data/` contains one folder per data set with the naming format [data description]_[data start year]-[data end year]_[FOIA year]-[FOIA month], not all data directories have the relevant files in them due to data sharing agreement, but they can be obtain through other means (discussed below).
  
 - `format/` contains one folder per sub-data set (e.g. complaints-accused while there is one folder in `raw_data` for complaints data from the 2016-11 FOIA) and this folder take input data from `raw_data/` and does initial cleaning and standardization. Formatting files can be run from the relevant data folder and running the command "python src/format.py".
 
 - `merge-officers` contains one folder per sub-data set for data with officer information, takes input data from `format/` sub-directory output folders, and iteratively merges data and must be run following the order in the `merge-officers/ORDER` file. Mergingg files can be run from the relevant data folder and running the command "python src/merge.py". Each merge folder before `officer-profiles/` will output both a copy of the input data with an additional `NUID` column (numeric unique ID) which connects officers across datasets and a `officer-reference.csv.gz` file which contains a collection of all observations of officers that have been seen across data sets --- for example, if one data set is missing race as a column or an officer changed their last name between data sets, `officer-reference.csv.gz` will reflect those differences across files. `officer-profiles/` takes the resulting `officer-reference.csv.gz` file and outputs a single aggregated `officer-profiles.csv.gz` for 1 observation per NUID (officer).
 
 - `create-panel-data` contains additional data processing files which do not have a specific order, take files from the output folders in `merge-officers` and `format`, and can be run from the relevant sub-folder by running the command "python src/create-panel-data.py".

## Instructions

1. Install python and the relevant modules
2. Download or clone this repository
3. If you wish to complete full replication, obtain all the relevant `raw_data/` files. Names of required but missing files due to data sharing agreement are listed in `missing_files.txt` within `raw_data/` subdirectories. They can be obtained either from the Invisible Institute or from the author of this repository with written permission from the Invisible Institute.
4. Repair any broken symlinks
5. Repair any symlinks in the `input` folders within each subdirectory. In each subdirectory in `format/`, run `python src/format.py`
6. Repair any symlinks in the `input` folders within each subdirectory. Following the designated order in `merge-officers/ORDER`, in each subdirectory in `merge-officers/`, run `python src/format.py`
7. Repair any symlinks in the `input` folders within each subdirectory. In each subdirectory in `create-panel-data/`, run `python src/create-panel-data.py`

