# pandas for handling data_frames
import pandas as pd
# used for efficient column naming
import numpy as np
# for folder creation
import os
################################################
# Variables to customise
################################################
# path of sources
path_data_sources = '/Users/dominik-cau/Documents/Lernen/Uni/Promotion/Projekte/' \
                    'Forschungsgruppe-Privacy/Datensätze.nosync/Agrarsubventionen/'


path_data_export = '/Users/dominik-cau/Documents/Lernen/Uni/Promotion/Projekte/' \
                    'Forschungsgruppe-Privacy/Datensätze.nosync/Agrarsubventionen/export/'

# source filename
csv_source_file_name = 'BPI Challenge 2018_shortened.csv'

# delimiter of csv-file
csv_delimiter = ';'

# Customisable variables have to be in the format:
# ['event_j', 'dynamic_feature_h_1', 'dynamic_feature_h_2']

# unique identifier column (static feature)
# examples: CaseID, serial number
# only give one unique identifier
unique_identifier = ['Case ID']

# constant attributes: attributes that are not necessarily unique but will be constant over the given time frame
# examples: age, gender
constant_attributes = ['(case) area', '(case) department']

# List of variable attributes to consider
variable_attributes = ['Activity', 'Complete Timestamp']

################################################
################################################

# read csv. data from disk
df_data = pd.read_csv(filepath_or_buffer=path_data_sources + csv_source_file_name, delimiter=csv_delimiter)

# drop all unnecessary columns
df_important_columns = df_data[unique_identifier + variable_attributes + constant_attributes]

# group data by unique identifier
df_grouped_by_identifier = df_important_columns.groupby(unique_identifier)

# enumerate all data in their respective column
df_enumerated_data = df_grouped_by_identifier.aggregate(lambda x: list(x))

# create list to store data frames of each attribute
list_of_data_frames = []

#
list_column_names = []

# loop through all constant attributes
for constant_attribute in constant_attributes:
    # create data frame from list
    # only keep first column: all the other columns are either be the same or n/a since it is a constant attribute
    list_of_data_frames.append(pd.DataFrame.from_records(df_enumerated_data[constant_attribute]).iloc[:, 0])

    # add column names andremove all whitespaces
    list_column_names.append(constant_attribute.replace(" ", ""))

# loop through all variable attributes
for variable_attribute in variable_attributes:
    # create data frame from list (from enumerated data) and save it in a list of data frames
    list_of_data_frames.append(pd.DataFrame.from_records(df_enumerated_data[variable_attribute]))

    # create meaningful header, use the attribute name and a number
    list_column_names.extend(np.core.defchararray.add(
        [variable_attribute.replace(" ", "")] * list_of_data_frames[-1].shape[1],
        np.array(range(0, list_of_data_frames[-1].shape[1]), dtype=str)))

# concatenate separate data frames to one data frame
df_for_export = pd.concat(list_of_data_frames, axis=1)

# rename columns
df_for_export.columns = list_column_names

# get index (unique identifier) from enumerated data
df_for_export.index = df_enumerated_data.index

# meaningful filename: original-filename + attributes
filename_beginning_part = csv_source_file_name.split(sep='.')[0]
filename_middle_part = '-'.join(constant_attributes)
filename_end_part = '-'.join(variable_attributes)

# remove whitespaces
filename_beginning_part = filename_beginning_part.replace(" ", "")
filename_middle_part = filename_middle_part.replace(" ", "")
filename_end_part = filename_end_part.replace(" ", "")

# in case only "constant_attribute" OR "variable_attribute" is given, replace double underscore with a single one
filename_concat = \
    (filename_beginning_part + '_' + filename_middle_part + '_' + filename_end_part + '.csv').replace("__", "_")

# if export folder does not exist, create the folder
if not os.path.exists(path_data_export):
    os.makedirs(path_data_export)

# write to disk
df_for_export.to_csv(path_or_buf=path_data_export + filename_concat)
