import os
import numpy as np
import pandas as pd
from functools import reduce
from .datapoint import load_df_data_from_folder
import itertools


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

def generate_grain_vocab_info(s):
    '''
        s: a Series (or a column of a dataframe) of all rec@fld value's grain_list.
            eg. grain_list: a list of tokens from raw sentence. 
        
        output: a dict of idx2v, v2dix, v2freq.
    '''
    value_list = itertools.chain(*s.to_list())
    d = pd.Series(value_list).value_counts().to_dict()
    v2freq = {'_padding': 0}
    if '_missing' in d:
        v2freq['_missing'] = d.pop('_missing') # I should have some prefix here. 
    for k, v in d.items(): v2freq[k] = v

    idx2v = {idx: v for idx, v in enumerate(v2freq.keys())}
    v2idx = {v:k for k, v in idx2v.items()}
    
    return {'idx2v': idx2v, 'v2idx': v2idx, 'v2freq': v2freq}


def get_compressed_df(df_rec, full_recfldgrn_name, prefix_ids):
    '''
        df_rec: with columns: prefix_ids + focal_ids + full_recfldgrn_name
            prefix_ids: [PID, ECID] 
            focal_ids: [PNID], current primary key
            full_recfldgrn_name: `PrimaryNote - Section - SectSent@Sentence - TkGrn`

            prefix_ids_new: [PID] 
            focal_ids_new: [ECID], current primary key
            full_recfldgrn_name_new: `EC - PrimaryNote - Section - SectSent@Sentence - TkGrn`
    '''
    CompressParent_ID = prefix_ids[-1] # parent layer's ID which will be used to compress. 
    prefix_ids_new = prefix_ids[:-1]   # the new prefix_ids. 

    df_rec_new = pd.DataFrame(df_rec.groupby(CompressParent_ID).apply(lambda x: x.to_dict('list')).to_list())

    for col in prefix_ids: 
        df_rec_new[col] = df_rec_new[col].apply(lambda x: x[0])
        
    full_recfldgrn_name_new = CompressParent_ID.replace('ID', '') + '-' + full_recfldgrn_name
    df_rec_new = df_rec_new.rename(columns = {full_recfldgrn_name: full_recfldgrn_name_new})
    df_rec_new = df_rec_new[prefix_ids + [full_recfldgrn_name_new]]
    return df_rec_new, full_recfldgrn_name_new, prefix_ids_new


def get_highorder_input_idx(df, recfldgrn_sfx, prefix_ids, focal_ids):
    '''
        df: raw data with column: prefix_ids + focal_ids + [recfldgrn_sfx]
        recfldgrn_sfx: xxx_wgt, or xxx_tknidx, or xxx_fldidx
        prefix_ids: [PID, ECID, PNID, PNSectID]
        focal_ids: [SectSentID]
    '''
    # recfldgrn = recfldgrn_sfx.split('_')[0]
    # rec, field = recfldgrn.split('-')[0].split('@')
    # grain = recfldgrn.split('-')[-1]

    # top_id = df.columns[0] # need to double check this.
    if len(prefix_ids) > 0:
        prefix_ids_c = prefix_ids.copy() # _c: copy (meaningless)
        df_Rec = df[prefix_ids_c + [recfldgrn_sfx]]
        for i in range(len(prefix_ids_c)):
            df_Rec, recfldgrn_sfx, prefix_ids_c = get_compressed_df(df_Rec, recfldgrn_sfx, prefix_ids_c)
    else:
        df_Rec = df[focal_ids + [recfldgrn_sfx]]

    df_p = df_Rec.reset_index(drop = True) # p: patient.
    return df_p


def generate_EmbedDict_from_processed_df_whole(df_whole, recfldgrn):
    EmbedDict = {}
    
    # tkn (key)
    sfx = 'tkn'
    embed_type = 'CateEmbed'
    recfldgrn_sfx = f'{recfldgrn}_{sfx}'
    # print(recfldgrn_sfx)
    s = df_whole[recfldgrn_sfx] 
    Vocab = generate_grain_vocab_info(s) 
    v2idx = Vocab['v2idx']
    vocab_size = len(v2idx)
    tknz = None
    # print(v2idx)
    d = {'embed_type':embed_type, 
         'recfldgrn_sfx':recfldgrn_sfx,
         'vocab_size': vocab_size,
         'Vocab': Vocab,
         'tknz': tknz}
    df_whole[f'{recfldgrn}_{sfx}idx'] =  df_whole[f'{recfldgrn}_{sfx}'].apply(lambda x: [ v2idx[i] for i in x])
    EmbedDict[sfx] = d
    
    # tpc (topic)
    sfx = 'tpc'
    embed_type = 'CateEmbed'
    recfldgrn_sfx = f'{recfldgrn}_{sfx}'
    # print(recfldgrn_sfx)
    s = df_whole[recfldgrn_sfx] 
    Vocab = generate_grain_vocab_info(s) 
    v2idx = Vocab['v2idx']
    vocab_size = len(v2idx)
    # print(v2idx)
    d = {'embed_type':embed_type, 
         'recfldgrn_sfx':recfldgrn_sfx,
         'vocab_size': vocab_size,
         'Vocab': Vocab,
         'tknz': tknz}

    df_whole[f'{recfldgrn}_{sfx}idx'] =  df_whole[f'{recfldgrn}_{sfx}'].apply(lambda x: [v2idx[i] for i in x])
    EmbedDict[sfx] = d
    return df_whole, EmbedDict
