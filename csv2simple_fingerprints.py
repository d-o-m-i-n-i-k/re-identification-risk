# pandas for handling data_frames
import pandas as pd
# used for efficient column naming
import numpy as np
# for folder creation
import os
# for time
import time
# for output
import csv

################################################
# Variables to customise
################################################
# path of sources
path_data_sources = '/Users/dominik-cau/Documents/Lernen/Uni/Promotion/Projekte/' \
                    'Forschungsgruppe-Privacy/Datensätze.nosync/Sepsis/'

path_data_export = '/Users/dominik-cau/Documents/Lernen/Uni/Promotion/Projekte/' \
                   'Forschungsgruppe-Privacy/Datensätze.nosync/Sepsis/export/' + \
                   time.strftime("%Y-%m-%d_%H-%M") + '/'

# source filename
csv_source_file_name = 'sepsis_raw.csv'

# delimiter of csv-file
csv_delimiter = ','

# Customisable variables have to be in the format:
# ['event_j', 'dynamic_feature_h_1', 'dynamic_feature_h_2']

# unique identifier column (static feature)
# examples: CaseID, serial number
# only give one unique identifier
unique_identifier = ['case']

# list of all the time stamp related columns
timestamps = ['startTime', 'completeTime']

# column name of the event description
event_sequence = 'event'
# list of columns to ignore
ignore_columns = []
fingerprint_variations = {'A': ['case_attributes', 'event_sequence', 'event_attributes', 'timestamps'],
                          'B': ['case_attributes', 'event_sequence', 'event_attributes'],
                          'C': ['case_attributes', 'event_sequence'],
                          'D': ['case_attributes', 'event_sequence', 'timestamps'],
                          'E': ['event_sequence', 'event_attributes', 'timestamps'],
                          'F': ['event_sequence', 'event_attributes'],
                          'G': ['event_sequence', 'timestamps'],
                          'H': ['event_sequence']}


################################################
################################################

# if export folder does not exist, create the folder
if not os.path.exists(path_data_export):
    os.makedirs(path_data_export)

with open(path_data_export + 'fingerprints.csv', "w", newline='') as csv_file:
    writer = csv.writer(csv_file, delimiter=',')
    for key, val in fingerprint_variations.items():
        writer.writerow([key, val])

# read csv. data from disk
df_data = pd.read_csv(filepath_or_buffer=path_data_sources + csv_source_file_name, delimiter=csv_delimiter)
# replace NaN with None
# not very elegant but since no numpy operations are needed good enough
df_data = df_data.where((pd.notnull(df_data)), None)

# columns_to_delete = np.array(unique_identifier + timestamps + ignore_columns)
columns_to_delete = np.array(unique_identifier + ignore_columns)

attributes = df_data.columns.values
for column in columns_to_delete:
    attributes = attributes[attributes != column]

# group data by unique identifier
df_grouped_by_identifier = df_data.groupby(unique_identifier)

# enumerate all data in their respective column
df_enumerated_data = df_grouped_by_identifier.aggregate(lambda x: list(x))

# loop through all cases
for fingerprint_ID, fingerprint_content in fingerprint_variations.items():

    try:
        fingerprint_content[fingerprint_content.index('event_sequence')] = event_sequence
        fingerprint_content[fingerprint_content.index('timestamps')] = timestamps
    except ValueError:
        pass

    # create list to store data frames of each attribute
    list_of_data_frames = []

    list_column_names = []

    # insert constant values in the beginning, but respect given order
    # use this variable to determine the insertion position
    constant_value_count = 0

    # loop through all variable attributes
    for attribute in attributes:
        # skip event or time stamp if required by fingerprint by going to next iteration
        if (attribute == event_sequence and attribute not in fingerprint_content) or \
                (attribute in timestamps and not any(attribute in content for content in fingerprint_content)):
            continue
        # create data frame from list (from enumerated data)
        df_current_iteration = pd.DataFrame.from_records(df_enumerated_data[attribute])

        # if attribute is constant only use it once and do not create multiple columns
        # determined by: count unique values for each row and drop 'None' values
        # if only the first column has a value or if all columns have the same value 'df.nunique' will
        # return '1'
        # if all 'df.nunique' returns for all rows '1' it will sum up to the number of rows
        # and therefore if those numbers are the same every row only contains one unique value
        if 'case_attributes' in fingerprint_content and \
                sum(df_current_iteration.nunique(dropna=True, axis=1)) == df_current_iteration.shape[0]:

            # get only first column. all other columns should either be empty or equal
            df_current_iteration = df_current_iteration.iloc[:, 0]

            # save it in a list of data frames
            list_of_data_frames.insert(constant_value_count, df_current_iteration)
            # create meaningful header, use the attribute name
            list_column_names.insert(constant_value_count, attribute.replace(" ", ""))

            # increase insertion position by one
            constant_value_count += 1
        # only add event attributes if the fingerprint requires it
        elif (('event_attributes' in fingerprint_content or
              (attribute == event_sequence and event_sequence in fingerprint_content)) and
                not sum(df_current_iteration.nunique(dropna=True, axis=1)) == df_current_iteration.shape[0]) \
                or (attribute in timestamps and any(attribute in content for content in fingerprint_content)):
            # save it in a list of data frames
            list_of_data_frames.append(df_current_iteration)
            # create meaningful header, use the attribute name and a number
            list_column_names.extend(np.core.defchararray.add(
                [attribute.replace(" ", "")] * list_of_data_frames[-1].shape[1],
                np.array(range(0, list_of_data_frames[-1].shape[1]), dtype=str)))

    # concatenate separate data frames to one data frame
    df_for_export = pd.concat(list_of_data_frames, axis=1)

    # rename columns
    df_for_export.columns = list_column_names

    # get index (unique identifier) from enumerated data
    df_for_export.index = df_enumerated_data.index

    # meaningful filename: original-filename + attributes
    filename_beginning_part = csv_source_file_name.split(sep='.')[0].replace(" ", "")

    # in case only "constant_attribute" OR "variable_attribute" is given, replace double underscore with a single one
    filename_concat = filename_beginning_part + '-Fingerprint_' + fingerprint_ID + '.csv'

    # write to disk
    df_for_export.to_csv(path_or_buf=path_data_export + filename_concat)

