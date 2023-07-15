import pandas as pd

def reshape_df_rec_2_df_DTln(dp, RecName, DTColDict, TimeUnitLength):
    # df['RecType'] = RecType
    for i in ['DT_s', 'DT_e', 'DT_dur', 'DT_r', 'DT_tz']:
        assert i in DTColDict

    # Part 1: 
    df = dp[RecName]# .iloc[0]
    cols_to_drop = []
    for new_DT_col, old_col in DTColDict.items():
        # print(new_DT_col, old_col)
        if old_col not in df.columns:
            df[new_DT_col] = None
        else:
            # print(df[old_col])
            df[new_DT_col] = df[old_col]
            cols_to_drop.append(old_col)
            
    df = df.drop(columns = cols_to_drop).reset_index(drop = True)
    
    # Following Parts are: changing values in the df.

    # Part 2: update DT_s
    # index = (df['DT_s'] <= pd.to_datetime('2010-01-01'))
    df['DT_s'] = df['DT_s'].apply(lambda x: None if x <= pd.to_datetime('2010-01-01') else x)
    # df.loc[index, 'DT_s'] = df.loc[index, 'DT_r']
    df['DT_s'] = df['DT_s'].fillna(df['DT_r'])
    
    # s = df.loc[index, 'DT_r']
    # df.loc[index, 'DT_s'] = s.values
    
    # Part 3: update DT_e
    # (a)
    index = (df['DT_e'].isna()) & (-df['DT_dur'].isna())
    df.loc[index, 'DT_e'] = df.loc[index, 'DT_s'] + pd.to_timedelta(df.loc[index, 'DT_dur'],  'm')
    # (b)
    df['DT_e'] = df['DT_e'].fillna(df['DT_s'])
    
    # Part 4: update DT_tz
    Pat_TimeZone = dp['P'].iloc[0]['UserTimeZoneOffset']
    df['DT_tz'] = df['DT_tz'].fillna(Pat_TimeZone)


    # Part 5: localize DT
    for i in ['DT_s', 'DT_e', 'DT_r']:
        df[i+ '_l'] = pd.to_datetime(df[i]) + pd.to_timedelta(df['DT_tz'], 'm')
        
    # Part 6: assign col
    df['RecName'] = RecName
    
    ##########################
    # TimeUnitLength = 5
    ##########################

    for DT_col in ['DT_s_l', 'DT_e_l', 'DT_r_l']:
        date = df[DT_col].dt.date.astype(str)
        hour = df[DT_col].dt.hour.astype(str)
        minutes = ((df[DT_col].dt.minute / TimeUnitLength).astype(int) * TimeUnitLength).astype(str)
        df[DT_col+'n'] = pd.to_datetime(date + ' ' + hour +':' + minutes + ':' + '00')

        
    # Part 6: Range the data.
    DT_cols = ['DT_s', 'DT_e', 'DT_r']
    DT_cols = [i+ '_ln' for i in DT_cols] + [i+ '_l' for i in DT_cols]
    prefix_cols = ['PID', RecName + 'ID'] + ['RecName'] + DT_cols + ['DT_dur', 'DT_tz']
    new_cols = prefix_cols + [i for i in df.columns if i not in prefix_cols]
    df = df[new_cols]
    df = df.sort_values('DT_s_l').reset_index(drop = True)
    return df

def get_flatten_df_rec(dp, TimeUnitLength):
    s, e = dp['DT_s_ln'], dp['DT_e_ln']
    num_units = int((e - s).total_seconds() / (TimeUnitLength* 60)) + 1
    d = {}
    d['DT_ln'] = [s + pd.to_timedelta(i *TimeUnitLength, 'm') for i in range(num_units)]
    for col in dp.keys():
        d[col] = [dp[col]] * num_units
    
    df = pd.DataFrame(d).reset_index(drop = True)
    return df

def reshape_df_DTln_2_df_DTidx(df, TimeUnitLength, **kwargs):
    l = df.apply(lambda dp: get_flatten_df_rec(dp, TimeUnitLength), axis = 1).to_list()
    dfx = pd.concat(l).reset_index(drop = True)
    dfx = dfx.groupby(['PID', 'DT_ln']).apply(lambda df: df.to_dict('records')).reset_index().rename(columns = {0: RecName})
    return dfx

