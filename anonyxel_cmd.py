#!/home/diego/Documents/DataScience/anonyxel/virtualenv_anonyxel/anonyxel/bin/python3.6
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
"""
Copyright 2018 Egor Kobylkin
Created on We 3. Okt 22:51:46 CEST 2018
@author: Egor Kobylkin
BSD 3-Clause "New" or "Revised" License bsd-3-clause
https://opensource.org/licenses/BSD-3-Clause
"""

# python3.6
#pip install pandas, xlrd, openpyxl, sklearn, tables

import os, argparse, pandas as pd, numpy as np
from sklearn import preprocessing
from argparse import RawTextHelpFormatter

###########################################################
# define the command line argument parser
# read it's description for usage documenation
###########################################################
# https://docs.python.org/2/howto/argparse.html
parser = argparse.ArgumentParser(

description='''Excel Sheet Data Anonymizer for Machine Learning.

Takes DATA_FILE.anon.xlsx with ID, Outcome and some data columns 
in the DATA worksheet. Hashes IDs, keeps Outcome(0/1) cleartext. 
Numbers(integers) excluding float are first hashed to a string.
Afterwards all categorical and string columns are encoded into levels. 
Floats are scaled. 

The Results are written to DATA_FILE.anon.xlsx. 

An ID map for joining back to the original data is written 
into DATA_FILE.id_map.xlsx'''
,formatter_class=RawTextHelpFormatter)


parser.add_argument('-d', '--data_file', nargs=1,
help='Excel File to anonymize with a single worksheet called DATA.'
                    )
parser.add_argument('-w', '--worksheet', nargs=1,
help='Excel worksheet to use. If omitted we will try to use the DATA worksheet.'
                    )
parser.add_argument('-i', '--id_column', nargs=1,
help='''A column that represents a Unique ID to identify your cases, 
must not repeat within the dataset.'''
                    )
parser.add_argument('-o', '--outcome_column', nargs=1,
help='''A 0/1 column that represents the outcome of your cases. Empty cells 
represent rows/cases that you want to predict. It will not be anonymized 
because we need it in clear text. '''
                    )

args = parser.parse_args()

data_file = ''
if args.data_file:
    data_file = args.data_file[0]
else:
    msg ='''A data file is a mandatory parameter! Use -d FILENAME or \
--data_file FILENAME.'''
    print(msg)#logger.error(msg)
    exit(8)
if (not (os.path.isfile(data_file))):
    msg = 'the input parameter file: ' + data_file + ' does not exist!'
    print(msg)#logger.error(msg)
    exit(8)

if args.worksheet:
    worksheet = args.worksheet[0]
else:
    worksheet='DATA'
    msg = 'No excel worksheet name supplied. Using "DATA"'
    print(msg)#logger.error(msg)

if args.id_column:
    id_column = args.id_column[0]
else:
    id_column='ID'
    msg = 'No special ID column name supplied. Using "ID"'
    print(msg)#logger.error(msg)

if args.outcome_column:
    outcome_column = args.outcome_column[0]
else:
    outcome_column='Outcome'
    msg = 'No special outcome column name supplied. Using "Outcome"'
    print(msg)#logger.error(msg)


###########################################################
# Reading in the data and preparing the dataframes
###########################################################
try:
    dfs = pd.read_excel(data_file, sheet_name=None) # reads all sheets
#TODO do something here to select the correct worksheet
    excel_df=dfs[worksheet]
except:
    msg = 'Could not read in data from file"' + data_file +'" with "' \
        +worksheet+'" or "DATA" worksheet!'
    print(msg)#logger.error(msg)
    exit(8)


###########################################################
# Where to put the results
###########################################################
orig_path,extention=os.path.splitext(data_file)
result_filename=orig_path+".anon"+extention
result_h5_filename=orig_path+".anon.h5"
ID_map_filename=orig_path+".id_map"+extention


###########################################################
# Anonymisation happens here
###########################################################

#set non-data columns aside for later
holdout_df=excel_df.loc[:,[id_column,outcome_column]] 

#drop non-data columns from the dataset
dataframe=excel_df.drop(columns=[id_column,outcome_column])

# helper function for hashing strings and integers
#https://docs.python.org/2/library/hashlib.html
import hashlib # TODO check what hash function is optimal
m = hashlib.blake2b(salt=os.urandom(hashlib.blake2b.SALT_SIZE)) 
def hash_to_string(cell_value):
    try: 
         m.update(cell_value.encode('utf8')) # string
         return m.hexdigest()
    except:
         m.update(bytes(cell_value)) # integer or float
         return m.hexdigest()

# numbers(integers) excluding float-> are first hashed to a string,
# afterwards categorical/string columns are encoded, floats are scaled.
# TODO add tokenisation for the text cells
for col in dataframe.select_dtypes(include=[np.number]
    , exclude=[np.float64]).columns:
    dataframe[col] = dataframe[col].map(hash_to_string).astype(np.object)

for col in dataframe.select_dtypes(exclude=[np.number]).columns: 
    dataframe[col] = dataframe[col].astype('category').cat.codes

for col in dataframe.select_dtypes(include=[np.float64]).columns:
    dataframe[col] = pd.DataFrame(preprocessing.scale(dataframe[col].values)
                                  ,index=dataframe[col].index, columns=[col,])

# and fill all missing data values with the most frequent value
dataframe=dataframe.fillna(dataframe.mode().iloc[0])

# rename columns 
dataframe.columns=[ 'column_'+str(clmn_num) for clmn_num,clmn_name 
                        in enumerate(dataframe.columns) ]

# construct full dataset but now with hashed IDs  
dataframe['Hashed_ID']=holdout_df[id_column].map(hash_to_string)
dataframe['Clear_Outcome']=holdout_df[outcome_column]
dataframe = dataframe.reindex(['Hashed_ID','Clear_Outcome'] 
          + list([a for a in dataframe.columns if a 
                    not in ['Hashed_ID','Clear_Outcome']]), axis=1)

###########################################################
# Anonymisation done
###########################################################

# Now we add the hashed ids to the original ones and build a map.
# With this map you can track back the results on your side with a VLOOKUP.
# Do_not share (upload) this file outside of your domain.
ID_map_df=holdout_df[[id_column]]
ID_map_df['Hashed_ID']=dataframe['Hashed_ID']


###########################################################
# Writing out the results
##########################################  #################
ID_map_df.to_excel(ID_map_filename, index = False)
dataframe.to_excel(result_filename, sheet_name = "Anonymized_Data"
    , index = False)
#lets writhe out a HDFS store that is expecially easy to process with pandas
h5container = pd.HDFStore(result_h5_filename) # this is the file cache
h5container['DATA'] = dataframe
h5container.close()


#with pd.HDFStore(filename, mode='r') as h5container:
#    dataframe = h5container.select('DATA')
