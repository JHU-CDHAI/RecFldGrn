import pandas as pd

import numpy as np

RecName = "P"

SynFld = "PECINfo"

GrnName = "InfoGrn"

rec_folder = "data/ProcData/RecFolder/"

RecName_Chain = ['P']

RecTableName2FldColumns_Dict = {'P': ['PID', 'age', 'basicInfo'], 'EC': ['DT_min', 'DT_max']}

reshape_fn_kwargs = {}

EmbedDict = {'tkn': {'embed_type': 'CateEmbed', 'recfldgrn_sfx': 'P@PECINfo-InfoGrn_tkn', 'vocab_size': 55, 'Vocab': {'idx2v': {0: '_padding', 1: 'age_miss', 2: 'ecDaysmiss', 3: 'ecDaysless', 4: 'ecDaysbottom', 5: 'ecDaystop', 6: 'age_more', 7: 'ecDaysB0+', 8: 'ecDaysB7+', 9: 'ecDaysB14+', 10: 'ecDaysB21+', 11: 'ecDaysB28+', 12: 'ecDaysB35+', 13: 'ecDaysB42+', 14: 'ecDaysB49+', 15: 'ecDaysB56+', 16: 'ecDaysB63+', 17: 'ecDaysB70+', 18: 'ecDaysB77+', 19: 'ecDaysB84+', 20: 'ecDaysB91+', 21: 'ecDaysB98+', 22: 'ecDaysmore', 23: 'ecDaysbase', 24: 'age_B18+', 25: 'age_B43+', 26: 'age_less', 27: 'age_bottom', 28: 'age_top', 29: 'age_base', 30: 'age_B23+', 31: 'age_B28+', 32: 'age_B38+', 33: 'age_B33+', 34: 'age_B48+', 35: 'age_B53+', 36: 'age_B58+', 37: 'age_B63+', 38: 'age_B68+', 39: 'age_B73+', 40: 'age_B78+', 41: 'B', 42: 'Male', 43: 'Female', 44: 'A', 45: 'D', 46: 'C', 47: 'ecNum_7', 48: 'ecNum_23', 49: 'ecNum_34', 50: 'ecNum_22', 51: 'ecNum_87', 52: 'ecNum_26', 53: 'ecNum_25', 54: 'ecNum_1'}, 'v2idx': {'_padding': 0, 'age_miss': 1, 'ecDaysmiss': 2, 'ecDaysless': 3, 'ecDaysbottom': 4, 'ecDaystop': 5, 'age_more': 6, 'ecDaysB0+': 7, 'ecDaysB7+': 8, 'ecDaysB14+': 9, 'ecDaysB21+': 10, 'ecDaysB28+': 11, 'ecDaysB35+': 12, 'ecDaysB42+': 13, 'ecDaysB49+': 14, 'ecDaysB56+': 15, 'ecDaysB63+': 16, 'ecDaysB70+': 17, 'ecDaysB77+': 18, 'ecDaysB84+': 19, 'ecDaysB91+': 20, 'ecDaysB98+': 21, 'ecDaysmore': 22, 'ecDaysbase': 23, 'age_B18+': 24, 'age_B43+': 25, 'age_less': 26, 'age_bottom': 27, 'age_top': 28, 'age_base': 29, 'age_B23+': 30, 'age_B28+': 31, 'age_B38+': 32, 'age_B33+': 33, 'age_B48+': 34, 'age_B53+': 35, 'age_B58+': 36, 'age_B63+': 37, 'age_B68+': 38, 'age_B73+': 39, 'age_B78+': 40, 'B': 41, 'Male': 42, 'Female': 43, 'A': 44, 'D': 45, 'C': 46, 'ecNum_7': 47, 'ecNum_23': 48, 'ecNum_34': 49, 'ecNum_22': 50, 'ecNum_87': 51, 'ecNum_26': 52, 'ecNum_25': 53, 'ecNum_1': 54}, 'v2freq': {'_padding': 0, 'age_miss': 8, 'ecDaysmiss': 8, 'ecDaysless': 8, 'ecDaysbottom': 8, 'ecDaystop': 8, 'age_more': 8, 'ecDaysB0+': 8, 'ecDaysB7+': 8, 'ecDaysB14+': 8, 'ecDaysB21+': 8, 'ecDaysB28+': 8, 'ecDaysB35+': 8, 'ecDaysB42+': 8, 'ecDaysB49+': 8, 'ecDaysB56+': 8, 'ecDaysB63+': 8, 'ecDaysB70+': 8, 'ecDaysB77+': 8, 'ecDaysB84+': 8, 'ecDaysB91+': 8, 'ecDaysB98+': 8, 'ecDaysmore': 8, 'ecDaysbase': 8, 'age_B18+': 8, 'age_B43+': 8, 'age_less': 8, 'age_bottom': 8, 'age_top': 8, 'age_base': 8, 'age_B23+': 8, 'age_B28+': 8, 'age_B38+': 8, 'age_B33+': 8, 'age_B48+': 8, 'age_B53+': 8, 'age_B58+': 8, 'age_B63+': 8, 'age_B68+': 8, 'age_B73+': 8, 'age_B78+': 8, 'B': 4, 'Male': 4, 'Female': 4, 'A': 2, 'D': 1, 'C': 1, 'ecNum_7': 1, 'ecNum_23': 1, 'ecNum_34': 1, 'ecNum_22': 1, 'ecNum_87': 1, 'ecNum_26': 1, 'ecNum_25': 1, 'ecNum_1': 1}}, 'tknz': None}, 'tpc': {'embed_type': 'CateEmbed', 'recfldgrn_sfx': 'P@PECINfo-InfoGrn_tpc', 'vocab_size': 5, 'Vocab': {'idx2v': {0: '_padding', 1: 'ecDays', 2: 'age', 3: 'basicInfo', 4: 'ecNum'}, 'v2idx': {'_padding': 0, 'ecDays': 1, 'age': 2, 'basicInfo': 3, 'ecNum': 4}, 'v2freq': {'_padding': 0, 'ecDays': 168, 'age': 152, 'basicInfo': 16, 'ecNum': 8}}, 'tknz': None}}

def grain_tkn_fn(dp):
    
    buffer_dict = {}
    
    # Patient Part: 
    df_recinput = dp['P'][['age', 'basicInfo']] # this special because xxx.
    
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
    buffer_dict[fld] = {'tkn': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # P@basicInfo
    fld = 'basicInfo'
    basicInfo_series = df_recinput[fld]# .mean()
    value = basicInfo_series.iloc[0]
    li_value_tkn = value.split('&')
    li_value_wgt = [1] * len(li_value_tkn)
    li_value_fld = [fld] * len(li_value_tkn)
    buffer_dict[fld] = {'tkn': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # EC Part
    df_recinput = dp['EC'][['DT_min', 'DT_max']]
    
    # P@ECNum
    fld = 'ecNum'
    value = len(df_recinput)
    li_value_tkn = [f'{fld}_{value}' ]
    li_value_wgt = [1] * len(li_value_tkn)
    li_value_fld = [fld] * len(li_value_tkn)
    buffer_dict[fld] = {'tkn': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # P@ecDays
    fld = 'ecDays'
    start, end, Min, Max, scale = [0, 100, 0, 500, 7]
    func_convert_Num_wgt = func_convert_Num_factory(start, end, Min, Max, scale)
    value = (df_recinput['DT_max'] - df_recinput['DT_min']).mean().total_seconds() / 60 / 60 / 24
    results = func_convert_Num_wgt(value)
    li_value_tkn = [f'{fld}{i}' for i in results[0]]
    li_value_wgt = results[1]
    li_value_fld = [fld] * len(li_value_tkn)
    buffer_dict[fld] = {'tkn': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # summary
    output = {}
    for i in ['tkn', 'wgt', 'tpc']:
        # output[i] = sum(, [])
        output[i.split('_')[-1]] = list(itertools.chain(*[d[i] for fld, d in buffer_dict.items()]))
        
    # key: tkn
    # value: wgt
    # fld: hypertype?
    return output


MetaDict = {"RecName": RecName,
"SynFld": SynFld,
"GrnName": GrnName,
"rec_folder": rec_folder,
"RecName_Chain": RecName_Chain,
"RecTableName2FldColumns_Dict": RecTableName2FldColumns_Dict,
"reshape_fn_kwargs": reshape_fn_kwargs,
"EmbedDict": EmbedDict,
"grain_tkn_fn": grain_tkn_fn}