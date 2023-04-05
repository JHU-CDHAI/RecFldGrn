import os
import random
import numpy as np
import pandas as pd

RANGE_SIZE = 10000


def get_df_rec(attr, path, old_columns, new_columns):
    df = pd.read_csv(path, low_memory = False)
    print(df.columns)
    df['RID'] = df.index
    for i in old_columns:
        if i not in  df.columns:
            df[i] = None
            
    df = df[old_columns]
    df.columns = new_columns
    # df = df.reset_index().rename(columns = {'index': 'RID'})
    DT_col = [i for i in new_columns if 'DT' in i][0]
    df[DT_col] = pd.to_datetime(df[DT_col])
    df = df.sort_values(['PID', DT_col]).reset_index(drop = True)
    # print(df.shape)
    df = df[df['ECID'] != 0].reset_index(drop = True)
    # print(df.shape)
    return df



import pandas as pd

def get_df_rec(attr, path, old_columns, new_columns):


    '''
    Function Name: get_df_rec

    Parameters:
    - attr: string
    - path: string
    - old_columns: list of strings
    - new_columns: list of strings

    Returns:
    - df: Pandas DataFrame object

    Description:
    This function reads a CSV file located at the specified path and returns a processed Pandas DataFrame object. 
    The DataFrame object is first printed to display the column names. 
    Then, a new column 'RID' is added to the DataFrame object, which is equal to the index of the DataFrame object. 
    For each column in old_columns, if it is not already present in the DataFrame object, 
    a new column with the column name is added to the DataFrame object with all its values set to None. 
    The DataFrame object is then filtered to only contain the columns in old_columns and renamed using the corresponding column names in new_columns. 
    A column with a name containing 'DT' is selected from new_columns, 
    and the values in that column are converted to Pandas datetime objects using the pd.to_datetime() function.
    The DataFrame object is sorted based on two columns: 
    'PID' and the datetime column selected in the previous step. 
    Finally, the DataFrame object is filtered to exclude any rows where the value of 'ECID' column is 0, 
    and the resulting DataFrame object is returned.


    '''
    # Read CSV file into a Pandas DataFrame object
    df = pd.read_csv(path, low_memory=False)
    # Print the column names of the DataFrame object
    print(df.columns)
    # Add a new column 'RID' to the DataFrame object, which is equal to the index of the DataFrame object
    df['RID'] = df.index
    # For each column in old_columns, if it is not already present in the DataFrame object, a new column with the column name is added to the DataFrame object with all its values set to None
    for i in old_columns:
        if i not in df.columns:
            df[i] = None
    # Filter the DataFrame object to only contain the columns in old_columns and rename the columns using the corresponding column names in new_columns
    df = df[old_columns]
    df.columns = new_columns
    # Select a column with a name containing 'DT' from new_columns and convert its values to Pandas datetime objects
    DT_col = [i for i in new_columns if 'DT' in i][0]
    df[DT_col] = pd.to_datetime(df[DT_col])
    # Sort the DataFrame object based on two columns: 'PID' and the datetime column selected in the previous step
    df = df.sort_values(['PID', DT_col]).reset_index(drop=True)
    # Filter the DataFrame object to exclude any rows where the value of 'ECID' column is 0
    df = df[df['ECID'] != 0].reset_index(drop=True)
    # Return the resulting DataFrame object
    return df


def convert_PID_to_PIDgroup(x, RANGE_SIZE):
    int_value = int(x.replace('P', ''))
    range_size = RANGE_SIZE
    group_idx = int(int_value / range_size)
    s = group_idx * range_size
    e = (group_idx + 1) * range_size - 1
    groupname = f'PIDGroup_s{s}_e{e}'
    return groupname

def write_df_to_folders(DataName, data_folder, df):
    dfy = df.copy()
    # data_folder = f'../Data/B-EHRDataSet/{DataName}'
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    dfy['GroupName'] = dfy['PID'].apply(lambda x: convert_PID_to_PIDgroup(x , RANGE_SIZE))
    for groupname, dfx in dfy.groupby('GroupName'):
        file = os.path.join(data_folder, f'{groupname}.p')
        # print(file)
        dfx = dfx.drop(columns= ['GroupName'])
        dfx.to_pickle(file)

def load_df_data_from_folder(data_folder):
    file_list = os.listdir(data_folder)
    # file_lis
    df = pd.concat([pd.read_pickle(os.path.join(data_folder, i)) for i in file_list]).reset_index(drop = True)
    return df# .shape


def get_dfx_p_filename(PID, DataName, data_folder, RANGE_SIZE):
    group_name = convert_PID_to_PIDgroup(PID, RANGE_SIZE) 
    p_filename = f'{data_folder}/{DataName}/{group_name}.p'
    return p_filename
    
def load_patient_data(PID, dfx):
    dfx = dfx[dfx['PID'] == PID].reset_index(drop = True)
    return dfx

def get_dfx_from_buffer(PID, DataName, data_folder, RANGE_SIZE, BUCKET_buffer):
    p_filename = get_dfx_p_filename(PID, DataName, data_folder, RANGE_SIZE)
    if p_filename not in BUCKET_buffer:
        dfx = pd.read_pickle(p_filename)
        BUCKET_buffer[p_filename] = dfx
        if len(BUCKET_buffer) > 100:
            for key in random.sample(BUCKET_buffer.keys(), 4):
                del BUCKET_buffer[key]
    else:
        dfx = BUCKET_buffer[p_filename]
    return dfx, BUCKET_buffer


class PatientDP(object):

    '''
    PatientDP: stands for the Patient Data Points. 

    one instance is a patient.

    the instance of the patient could quickly the his/her record information by using `get_df_rec`.

    '''
    BUCKET_buffer = {}

    def __init__(self, PID, data_folder, RANGE_SIZE):
        self.PID = PID
        self.data_folder = data_folder
        self.df_rec_dict = {}
        self.RANGE_SIZE = RANGE_SIZE
        
    def get_df_rec(self, DataName):
        '''
        DataName: the name of the record, like: P, EC, BMI, PN, PNSect, etc.
        '''
        if DataName not in self.df_rec_dict:
            dfx, BUCKET_buffer = get_dfx_from_buffer(self.PID, DataName, 
                                                     self.data_folder, 
                                                     self.RANGE_SIZE, 
                                                     self.BUCKET_buffer)
            self.BUCKET_buffer = BUCKET_buffer
            df_p = load_patient_data(self.PID, dfx)
            self.df_rec_dict[DataName] = df_p
            return df_p
        else:
            return self.df_rec_dict[DataName]
    
        
        
    
    