import pandas as pd

import numpy as np

import itertools

RecName = "P"

SynFld = "PECInfo"

GrnName = "InfoGrn"

rec_folder = "data/ProcData/RecFolder/"

RecName_Chain = ['P']

RecTableName2FldColumns_Dict = {'P': ['PID', 'age', 'basicInfo'], 'EC': ['DT_min', 'DT_max']}

reshape_fn_kwargs = {}

def grain_tkn_fn(row):
    import itertools
    
    buffer_dict = {}
    
    # Patient Part: 
    df_recinput = row['P'][['age', 'basicInfo']] # this special because xxx.
    
    # P@Age
    fld = 'age'
    start, end, Min, Max, scale = [18, 80, 1, 120, 5]
    func_convert_Num_wgt = func_convert_Num_factory(start, end, Min, Max, scale)
    value = df_recinput[fld].iloc[0]
    results = func_convert_Num_wgt(value)
    li_value_tkn = [f'{fld}_{i}' for i in results[0]]
    li_value_wgt = results[1]
    # print(len(li_value_tkn), len(li_value_wgt))
    li_value_fld = [fld] * len(li_value_tkn)
    buffer_dict[fld] = {'key': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # P@basicInfo
    fld = 'basicInfo'
    basicInfo_series = df_recinput[fld]# .mean()
    value = basicInfo_series.iloc[0]
    li_value_tkn = value.split('&')
    li_value_wgt = [1] * len(li_value_tkn)
    li_value_fld = [fld] * len(li_value_tkn)
    buffer_dict[fld] = {'key': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # EC Part
    df_recinput = row['EC'][['DT_min', 'DT_max']]
    
    # P@ECNum
    fld = 'ecNum'
    value = len(df_recinput)
    li_value_tkn = [f'{fld}_{value}' ]
    li_value_wgt = [1] * len(li_value_tkn)
    li_value_fld = [fld] * len(li_value_tkn)
    buffer_dict[fld] = {'key': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # P@ecDays
    fld = 'ecDays'
    start, end, Min, Max, scale = [0, 100, 0, 500, 7]
    func_convert_Num_wgt = func_convert_Num_factory(start, end, Min, Max, scale)
    value = (df_recinput['DT_max'] - df_recinput['DT_min']).mean().total_seconds() / 60 / 60 / 24
    results = func_convert_Num_wgt(value)
    li_value_tkn = [f'{fld}{i}' for i in results[0]]
    li_value_wgt = results[1]
    li_value_fld = [fld] * len(li_value_tkn)
    buffer_dict[fld] = {'key': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # summary
    output = {}
    for i in ['key', 'wgt', 'tpc']:
        # output[i] = sum(, [])
        output[i.split('_')[-1]] = list(itertools.chain(*[d[i] for fld, d in buffer_dict.items()]))
        
    # key: tkn
    # value: wgt
    # fld: hypertype?
    return output


def func_convert_Num_factory(start, end, Min, Max, scale):
    
    def func_convert_NUMgrn(v):

        miss, more, less, bottom, top = 0, 0, 0, 0, 0; base = 1
        
        if pd.isna(v) == True:
            miss = 1; base = 0
            num_embed = int((end - start) / scale)
            x = np.zeros(num_embed + 1)# .astype(int)
            x = list(x)
            wgt = [miss, more, less, bottom, top, base] + x
            tkn = ['miss', 'more', 'less', 'bottom', 'top', 'base'] + [f'B{start + i*scale}+' for i in range(len(x))]
            return tkn, wgt
        
        else:
            if type(v) == str:
                if v[0] not in '<>-': v = float(v) # No special symbols
                elif v[0] == '<': less = 1; v = float(v[1:]) # if < is here
                elif v[0] == '>': more = 1; v = float(v[1:]) # if > is here
                
            if v < start:  bottom = (start - v) / (start - Min); base = 0; v = start
            elif v > end: top = (v-end) / (Max-end); v = end

            num_embed = int((end - start) / scale)
            x = np.zeros(num_embed + 1)# .astype(f)
            num = int((v - start) / scale)
            # print(num)
            x[:num] = 1
            # print(v, start, scale,  v - start, num)
            # print( (v-start) - num * scale)
            x[num] = ((v-start) - num * scale) / scale
            x = x.round(4)
            # print((v - start) % scale, v, start, scale)
            # print(x[num])
            # print(x)
            x = list(x) # value part

            wgt = [miss, more, less, bottom, top, base] + x
            tkn = ['miss', 'more', 'less', 'bottom', 'top', 'base'] +[f'B{start + i*scale}+' for i in range(len(x))]
            return tkn, wgt
    return func_convert_NUMgrn


MetaDict = {"RecName": RecName,
"SynFld": SynFld,
"GrnName": GrnName,
"rec_folder": rec_folder,
"RecName_Chain": RecName_Chain,
"RecTableName2FldColumns_Dict": RecTableName2FldColumns_Dict,
"reshape_fn_kwargs": reshape_fn_kwargs,
"grain_tkn_fn": grain_tkn_fn,
"func_convert_Num_factory": func_convert_Num_factory}