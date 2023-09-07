import os 
import random
import pandas as pd

RANGE_SIZE = 10000 # This need to be adjusted for different RecFld Folder.

# Convert PID to PID Group
def convert_PID_to_PIDgroup(x, RANGE_SIZE):
    # x here is the PID, it can be a string (P0002) or int.
    if type(x) == str:
        if 'P' in x:
            int_value = int(x.replace('P', ''))
        else:
            int_value = int(x)
    else:
        int_value = int(x)
    range_size = RANGE_SIZE
    group_idx = int(int_value / range_size)
    s = group_idx * range_size
    e = (group_idx + 1) * range_size - 1
    groupname = f'Group_s{s}_e{e}'
    return groupname

# write df to folder
def write_df_to_folders(data_folder, df, IDNAME, RANGE_SIZE = RANGE_SIZE):
    dfy = df.copy()
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    dfy['GroupName'] = dfy[IDNAME].apply(lambda x: convert_PID_to_PIDgroup(x , RANGE_SIZE))
    for groupname, dfx in dfy.groupby('GroupName'):
        file = os.path.join(data_folder, f'{groupname}.p')
        # print(file)
        dfx = dfx.drop(columns= ['GroupName'])
        dfx.to_pickle(file)
        
# load df from folder
def load_df_data_from_folder(data_folder, **kwargs):
    # data_folder: = recfld_folder + RecName
    # rec_folder: the folder to hold many Rec types.
    file_list = os.listdir(data_folder)
    df = pd.concat([pd.read_pickle(os.path.join(data_folder, i)) for i in file_list if '.p' in i]).reset_index(drop = True)
    return df


def write_df_data_with_RecName(df, rec_folder, RecName, IDNAME, RANGE_SIZE = RANGE_SIZE):
    fullrec_folder = os.path.join(rec_folder, RecName)
    if not os.path.exists(fullrec_folder): os.makedirs(fullrec_folder)

    dfy = df.copy()
    dfy['GroupName'] = dfy[IDNAME].apply(lambda x: convert_PID_to_PIDgroup(x , RANGE_SIZE))
    for groupname, dfx in dfy.groupby('GroupName'):
        file = os.path.join(fullrec_folder, f'{groupname}.p')
        dfx = dfx.drop(columns= ['GroupName'])
        dfx.to_pickle(file)

def load_df_data_with_RecName(rec_folder, RecName):
    data_folder = os.path.join(rec_folder, RecName)
    file_list = sorted(os.listdir(data_folder))
    df = pd.concat([pd.read_pickle(os.path.join(data_folder, i)) for i in file_list if '.p' in i]).reset_index(drop = True)
    return df

def get_dfx_p_filename(IDValue, RecName, rec_folder, RANGE_SIZE):
    group_name = convert_PID_to_PIDgroup(IDValue, RANGE_SIZE) 
    p_filename = f'{rec_folder}/{RecName}/{group_name}.p'
    return p_filename
    
def load_dp_data(IDName, IDValue, dfx):
    # current Jul 15, 2023: All IDName must be 'PID', all IDValue is PID's value
    # IDName: PID, IDValue: value of PID
    dfx = dfx[dfx[IDName] == IDValue].reset_index(drop = True)
    return dfx

def get_dfx_from_buffer(IDName, IDValue, DataName, data_folder, RANGE_SIZE, BUCKET_buffer):
    p_filename = get_dfx_p_filename(IDValue, DataName, data_folder, RANGE_SIZE)
    if p_filename not in BUCKET_buffer:
        dfx = pd.read_pickle(p_filename)
        BUCKET_buffer[p_filename] = dfx
        if len(BUCKET_buffer) > 100:
            for key in random.sample(BUCKET_buffer.keys(), 4):
                del BUCKET_buffer[key]
    else:
        dfx = BUCKET_buffer[p_filename]
    return dfx, BUCKET_buffer


class DataPoint(object):

    '''
    DataPoint: stands for one data point. 
    one instance is a patient.
    the instance of the patient could quickly the his/her record information by using `get_df_rec`.
    '''
    BUCKET_buffer = {}

    def __init__(self, IDName, IDValue, rec_folder, RANGE_SIZE):
        self.IDName = IDName
        self.IDValue = IDValue
        self.rec_folder = rec_folder
        self.df_rec_dict = {}
        self.RANGE_SIZE = RANGE_SIZE
        
    def get_df_rec(self, RecName):
        '''
        RecName: the name of the record, like: P, EC, BMI, PN, PNSect, etc.
        '''
        if RecName not in self.df_rec_dict:
            dfx, BUCKET_buffer = get_dfx_from_buffer(self.IDName, self.IDValue, 
                                                     RecName, 
                                                     self.rec_folder, 
                                                     self.RANGE_SIZE, 
                                                     self.BUCKET_buffer)
            self.BUCKET_buffer = BUCKET_buffer
            df_p = load_dp_data(self.IDName, self.IDValue, dfx)
            self.df_rec_dict[RecName] = df_p
            return df_p
        else:
            return self.df_rec_dict[RecName]
        
    def __repr__(self):
        return f'<{self.IDName} [{self.IDValue}]>'
    