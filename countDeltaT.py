import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import os
import sys

def cleanCSV(df):
  arr_col = []
  for col in df.columns:
    if df[col].isna().all():
        print("Column", col, "contains only NaN values.")
        arr_col.append(col)
  df_cleaned = df.drop(columns=arr_col)
  df_cleaned = df_cleaned.replace('---', 0)
  return df_cleaned

def sortDf(df):
    if 'TIMESTAMP' in df.columns:
        df = df.sort_values('TIMESTAMP',ascending=True)
        df.index = range(len(df))
    else:
        print("Column TIMESTAMP is not in the dataframe.")
        return None
    return df

def getDeltaT(df):
    dfNoNan = df.dropna().reset_index(drop=True)
    deltaT = []
    for c in range(len(dfNoNan)-1):
        date1 = dfNoNan.loc[c]['dt']
        date2 = dfNoNan.loc[c+1]['dt']
        delta = date2 - date1
        delta = delta.total_seconds()
        deltaT.append(delta)
    deltaT = np.array(deltaT)
    avg_deltaT = deltaT.mean()
    days = avg_deltaT / 86400
    return days

def getKeysToDel(dictionary):
    keys_to_delete = []
    keys_to_delete.append('TIMESTAMP')
    keys_to_delete.append('dt')
    for key in dictionary.keys():
        if len(dictionary[key]) <= 2:
            keys_to_delete.append(key)
    return keys_to_delete

def getDictDeltaT(df):
    dict_deltaT = {}
    for col in df.columns:
        dict_deltaT[col] = None

    for col in df.columns:
        if col != 'dt' and col != 'TIMESTAMP':
            # print(col)
            df_col = df[[col,'dt']]
            days = getDeltaT(df_col)
            dict_deltaT[col] = days

    keys_to_delete = getKeysToDel(dict_deltaT)
    for key in keys_to_delete:
        del dict_deltaT[key]

    return dict_deltaT

def getAvgDeltaT(dict_deltaT):
    arr_values = list(dict_deltaT.values())
    arr_values = np.array(arr_values)
    avg_deltaT = arr_values.mean()

    return avg_deltaT

if __name__ == '__main__':

    parameter_list = sys.argv


    if '--file1'in parameter_list:
        file1 = os.path.abspath(sys.argv[parameter_list.index("--file1")+1])
    else:
        print("Mandatory parameter --file1 not found please check input parameters")
        sys.exit()

    df = pd.read_csv(file1)
    df['dt'] = pd.to_datetime(df['TIMESTAMP'],unit='ms')

    df_sorted = sortDf(df)
    df_sorted = cleanCSV(df_sorted)

    dict_deltaT = getDictDeltaT(df_sorted)
    avg_deltaT = getAvgDeltaT(dict_deltaT)

    print("Average deltaT:", avg_deltaT, "days")