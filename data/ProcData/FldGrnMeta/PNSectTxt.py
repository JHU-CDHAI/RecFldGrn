import pandas as pd

import numpy as np

import itertools

RecName = "PNSect"

SynFld = "PNSectTxt"

GrnName = "TknzGrn"

rec_folder = "data/ProcData/RecFolder/"

RecName_Chain = ['P', 'EC', 'PNSect']

RecTableName2FldColumns_Dict = {'EC': ['PID', 'ECID', 'DT_min', 'DT_max'], 'PN': ['PID', 'ECID', 'PNID', 'DT'], 'PNSect': ['PID', 'ECID', 'PNSectID', 'SectName'], 'PNSectSent': ['PID', 'ECID', 'PNSectID', 'PNSectSentID', 'Sentence']}

reshape_fn_kwargs = {}

def grain_tkn_fn(row):
    # please notice here that the information within dp is already filtered.
    
    # (0) Set up.
    import itertools
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
    max_length = 512
    used_leng = 0 # how many token positions used so far. 
    buffer_dict = {}
    
    # (A) PNSect Part: 
    df = row['PNSect']
    # (A.1) PNSect@SectName
    fld = 'SectName'
    if len(df) > 0:
        df_recinput = df[['SectName']].fillna('No section sentences')
        value = df_recinput[fld].iloc[0] # section name
    else:
        fld = 'SectName'
        value = 'No sections'
        
    current_max_leng = max_length - used_leng # new line
    li_value_tkn = tokenizer.tokenize(value, truncation=True, max_length=current_max_leng, add_special_tokens=True)
    li_value_wgt = [1] * len(li_value_tkn)
    li_value_fld = [fld] * len(li_value_tkn)
    used_leng = len(li_value_tkn)   # new line
    buffer_dict[fld] = {'key': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # (B) PNSectSent Part
    df = row['PNSectSent']
    # (B.1) PN-PNSectSent
    fld = 'Sentence'
    if len(df) > 0:
        df_recinput = df[['Sentence']].fillna('No section sentences')
        value = '\n'.join(df_recinput['Sentence'].to_list())
    else:
        value = 'No section sentences'
    current_max_leng = max_length - used_leng # new line
    li_value_tkn = tokenizer.tokenize(value, truncation=True, max_length=current_max_leng, add_special_tokens=True)
    li_value_wgt = [1] * len(li_value_tkn)
    li_value_fld = [fld] * len(li_value_tkn)
    used_leng = len(li_value_tkn)   # new line
    buffer_dict[fld] = {'key': li_value_tkn, 'wgt': li_value_wgt, 'tpc':li_value_fld}
    
    # summary
    output = {}
    for i in ['key', 'wgt', 'tpc']:
        output[i.split('_')[-1]] = list(itertools.chain(*[d[i] for fld, d in buffer_dict.items()]))
    return output


MetaDict = {"RecName": RecName,
"SynFld": SynFld,
"GrnName": GrnName,
"rec_folder": rec_folder,
"RecName_Chain": RecName_Chain,
"RecTableName2FldColumns_Dict": RecTableName2FldColumns_Dict,
"reshape_fn_kwargs": reshape_fn_kwargs,
"grain_tkn_fn": grain_tkn_fn}