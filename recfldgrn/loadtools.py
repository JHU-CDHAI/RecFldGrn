import os
import numpy as np
import pandas as pd
from functools import reduce
import pickle
import inspect
import importlib.util
from .datapoint import load_df_data_from_folder

def retrieve_name(var):
    """
    Gets the name of var. Does it from the out most frame inner-wards.
    :param var: variable to get name from.
    :return: string
    """
    for fi in reversed(inspect.stack()):
        names = [var_name for var_name, var_val in fi.frame.f_locals.items() if var_val is var and var_name[0] != '_']
        # print(names)
        if len(names) > 0:
            return names[0]
        

def convert_variables_to_pystirng(string_variables = [], 
                                  iterative_variables = [], 
                                  fn_variables = [], 
                                  prefix = ['import pandas as pd', 
                                            'import numpy as np']):
    L = prefix
    
    for i in string_variables:
        line = f'{retrieve_name(i)} = "{i}"'
        L.append(line)
        
    for i in iterative_variables:
        line = f'{retrieve_name(i)} = {i}'
        L.append(line)
        
    for i in fn_variables:
        line = f'{i.fn_string}'
        L.append(line)
        

    D_str = "\n\nMetaDict = {" + ',\n'.join(
                    [f'"{retrieve_name(i)}": {retrieve_name(i)}'
                 for i in string_variables + iterative_variables + fn_variables]
                ) + "}"
     
    python_strings = '\n\n'.join(L) + D_str
    
    return python_strings


def load_module_variables(file_path):

    # Create a module specification
    spec = importlib.util.spec_from_file_location("module_name", file_path)

    # Create a module from the specification
    module = importlib.util.module_from_spec(spec)

    # Load the module
    spec.loader.exec_module(module)
    return module


def save_to_pickle(obj, filename):
    with open(filename, 'wb') as f:  # write in binary mode
        pickle.dump(obj, f)

def load_from_pickle(filename):
    with open(filename, 'rb') as f:  # read in binary mode
        return pickle.load(f)
    

def get_df_whole_from_settings(RecName_Chain, 
                               RecTableName2FldColumns_Dict, 
                               rec_folder, 
                               RecName_Chain_to_RecName = {}):
   # (1) get df_prefix
    L = []

    RecNameID_Chain = [f'{i}ID' for i in RecName_Chain]
    for idx, RecName in enumerate(RecName_Chain):
        RecName_Full = RecName_Chain_to_RecName.get(RecName, RecName)
        df = load_df_data_from_folder(os.path.join(rec_folder, RecName_Full)) # [prefix_ids + focal_ids]
        df = df[RecNameID_Chain[:idx+ 1]].astype(str)
        L.append(df)

    df_prefix = reduce(lambda left, right: pd.merge(left, right, how = 'left'), L)
    
    for RecID in RecNameID_Chain:
        s = 'M' + pd.Series(df_prefix.index).astype(str)
        df_prefix[RecID] = df_prefix[RecID].fillna(s)
        
    # (2) get df_data
    RecLevel = RecName_Chain[-1]
    RecLevelID = f'{RecLevel}ID'

    L = []
    for RecName, FldList in RecTableName2FldColumns_Dict.items():
        df = load_df_data_from_folder(os.path.join(rec_folder, RecName))
        if FldList == 'ALL': FldList = list(df.columns)
        full_cols = [i for i in RecNameID_Chain if i not in FldList] + FldList
        full_cols = [i for i in full_cols if i in df.columns]
        df = df[full_cols]

        for RecID in RecNameID_Chain:
            if RecID in df.columns: df[RecID] = df[RecID].astype(str)

        if RecLevelID not in df.columns:
            on_cols = [i for i in df_prefix.columns if i in df.columns]
            df = pd.merge(df_prefix, df, on = on_cols, how = 'left')

        df = pd.DataFrame([{RecLevelID: RecLevelIDValue, RecName: df_input} 
                           for RecLevelIDValue, df_input in df.groupby(RecLevelID)])
        L.append(df)
        
    # Merge the dataframes in the list using reduce
    df_data = reduce(lambda left, right: pd.merge(left, right, on=RecLevelID, how = 'left'), L)

    # 3. get df_whole
    df_whole = pd.merge(df_prefix, df_data, on = RecLevelID, how = 'left')

    for RecName in RecTableName2FldColumns_Dict:
        ind = df_whole[RecName].isna()
        columns = df_whole[-ind][RecName].iloc[0].columns
        df_whole[RecName] = df_whole[RecName].apply(lambda x: x if type(x) == type(pd.DataFrame()) 
                                                    else pd.DataFrame(columns = columns))
    
    return df_whole



def get_df_individual_from_settings(RecName_Chain, 
                                    RecTableName2FldColumns_Dict, 
                                    Pat, 
                                    RecName_Chain_to_RecName = {}):
   # (1) get df_prefix
    L = []

    RecNameID_Chain = [f'{i}ID' for i in RecName_Chain]
    for idx, RecName in enumerate(RecName_Chain):
        RecName_Full = RecName_Chain_to_RecName.get(RecName, RecName)
        # df = load_df_data_from_folder(os.path.join(rec_folder, RecName_Full)) # [prefix_ids + focal_ids]
        df = Pat.get_df_rec(RecName_Full)
        df = df[RecNameID_Chain[:idx+ 1]].astype(str)
        L.append(df)

    df_prefix = reduce(lambda left, right: pd.merge(left, right, how = 'left'), L)
    
    for RecID in RecNameID_Chain:
        s = 'M' + pd.Series(df_prefix.index).astype(str)
        df_prefix[RecID] = df_prefix[RecID].fillna(s)
        
    # (2) get df_data
    RecLevel = RecName_Chain[-1]
    RecLevelID = f'{RecLevel}ID'

    L = []
    for RecName, FldList in RecTableName2FldColumns_Dict.items():
        # df = load_df_data_from_folder(os.path.join(rec_folder, RecName))
        df = Pat.get_df_rec(RecName_Full)
        if FldList == 'ALL': FldList = list(df.columns)
        full_cols = [i for i in RecNameID_Chain if i not in FldList] + FldList
        full_cols = [i for i in full_cols if i in df.columns]
        df = df[full_cols]

        for RecID in RecNameID_Chain:
            if RecID in df.columns: df[RecID] = df[RecID].astype(str)

        if RecLevelID not in df.columns:
            on_cols = [i for i in df_prefix.columns if i in df.columns]
            df = pd.merge(df_prefix, df, on = on_cols, how = 'left')

        df = pd.DataFrame([{RecLevelID: RecLevelIDValue, RecName: df_input} 
                           for RecLevelIDValue, df_input in df.groupby(RecLevelID)])
        L.append(df)
        
    # Merge the dataframes in the list using reduce
    df_data = reduce(lambda left, right: pd.merge(left, right, on=RecLevelID, how = 'left'), L)

    # 3. get df_whole
    df_whole = pd.merge(df_prefix, df_data, on = RecLevelID, how = 'left')

    for RecName in RecTableName2FldColumns_Dict:
        ind = df_whole[RecName].isna()
        columns = df_whole[-ind][RecName].iloc[0].columns
        df_whole[RecName] = df_whole[RecName].apply(lambda x: x if type(x) == type(pd.DataFrame()) 
                                                    else pd.DataFrame(columns = columns))
    
    return df_whole