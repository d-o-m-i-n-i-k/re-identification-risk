# pandas for handling data_frames
import pandas as pd
# used for efficient column naming
import numpy as np
# for folder creation
import os
import csv
from itertools import combinations

################################################
# Variables to customise
################################################
# path of sources
path_data_source = '/home/sanun001/Downloads/procecess-mining/export/'

path_data_uniqueness_export = '/home/sanun001/Downloads/procecess-mining/export/result/'

# source filename
csv_source_file_name = 'sepsis_age-activity-crp-diagnose-diagnosticblood.csv'

# delimiter of csv-file
csv_delimiter = ','

# Customisable variables have to be in the format:
# ['event_j', 'dynamic_feature_h_1', 'dynamic_feature_h_2']

# unique identifier --> re-identification risk would be 100%
# examples: CaseID, serial number
unique_identifier = ['case_id']


################################################
################################################

# read csv. data from disk
df_data = pd.read_csv(filepath_or_buffer=path_data_source + csv_source_file_name, delimiter=csv_delimiter, low_memory=False)

attributes=attributes = ['case_id','age','activity0','activity1','activity2','countactivity','diagnosticblood0','crp0','crp1','crp2','diagnose0'] 
#list(df_data.columns)
attributes_remaining = [x for x in attributes if x not in unique_identifier]

list_of_key_attributes = []

# create list to store data frames of each attribute
list_of_data_frames = []
list_column_names = ['quasi-identifier','min anonymity set','re-identification risk']
sample_size=len(df_data)

for i in range(1, len(list(attributes_remaining))+1):#for each combination of key values
    for combo in combinations(attributes_remaining, i):  # 2 for pairs, 3 for triplets, etc
        
        #count similiar rows
        dfdata=df_data.groupby(list(combo)).size().reset_index(name='frequency')
        
        unique_items = dfdata.apply(lambda x: True if x['frequency'] == 1 else False , axis=1)
        # Count number of unique (frequency=1) rows
        dfdata['num_of_uniques'] = len(unique_items[unique_items == True].index)

        list_of_key_attributes = list(combo)
        quasi_identifier=','.join(map(str, list_of_key_attributes))
        if not dfdata['frequency'].empty:
            list_of_data_frames.append([quasi_identifier,min(dfdata['frequency']),max(dfdata['num_of_uniques']/sample_size)])

df_for_export =   pd.DataFrame(list_of_data_frames)

# rename columns
df_for_export.columns = list_column_names

# meaningful filename: original-filename 
filename = csv_source_file_name.split(sep='.')[0].replace(" ", "") + '_results.csv'


# if export folder does not exist, create the folder
if not os.path.exists(path_data_uniqueness_export):
    os.makedirs(path_data_uniqueness_export)

# write to disk
df_for_export.to_csv(path_or_buf=path_data_uniqueness_export + filename,index=False, quoting= csv.QUOTE_NONNUMERIC)
print("ready")
