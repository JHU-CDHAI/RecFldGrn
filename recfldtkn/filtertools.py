def assign_dfwhole_obs_pred_fut_date(df_whole, DTtable, DTcol, BfRange = None, AfRange = None):
    predidx_date = df_whole[DTtable].apply(lambda x: x[DTcol].iloc[0])
    df_whole['predind_date'] = predidx_date
    
    RangeList = [i for i in [BfRange, AfRange] if i != None]
    
    if len(RangeList) == 0: 
        raise ValueError(f'Range Options is zero, you need to provide at least one of Bf and Af')
       
    for Range in RangeList:
        RangeWhSign = Range.replace('BF', '-').replace('AF', '')
        if RangeWhSign[-1] == 'y':
            number = int(RangeWhSign.replace('y', ''))
            days = number * 365
        elif RangeWhSign[-1] == 'd':
            number = int(RangeWhSign.replace('d', ''))
            days = number
        else:
            raise ValueError(f'The unit in Range {Range} is not available yet')

        bound_date = predidx_date.apply(lambda x: x + pd.to_timedelta(days, unit = 'days'))
        if number > 0: 
            df_whole['futind_date'] = bound_date; 
        else:  
            df_whole['obsind_date'] = bound_date; 

    return df_whole


def filter_dfwhole_content_with_start_end_date(df_whole, start_col, end_col, Rec2TimeCols_dfwhole_content):
    
    for Rec, TimeCols in Rec2TimeCols_dfwhole_content.items():
        for TimeCol in TimeCols:
            df_whole['ts'] = df_whole[Rec].apply(lambda x: x[TimeCol].iloc[0])
            # TODO: how to customized this index generation.
            index = - (df_whole.apply(lambda row: row['ts'] < row[start_col] or row['ts'] > row[end_col], axis = 1))
            df_whole = df_whole[index].reset_index(drop = True)
             
    df_whole = df_whole.drop(columns = ['ts'])
    
    return df_whole


def filter_row_content_with_start_end_date(row, start_col, end_col, Rec2TimeCols_row_content):
    
    for Rec, TimeCols_list in Rec2TimeCols_row_content.items():
        
        for TimeCol in TimeCols_list:
            df = row[Rec]; df['ts'] = df[TimeCol]
            # TODO: how to customized this index generation. 
            index = - (df.apply(lambda x: x['ts'] < row[start_col] or x['ts'] > row[end_col], axis = 1)) # considering missing records
            df = df[index].reset_index(drop = True)
            df = df.drop(columns = ['ts']); row[Rec] = df
        
    return row
