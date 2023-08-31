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

def resampleDf(df,datetime):
    if 'dt' in df.columns:
        df.set_index('dt', inplace=True)
    df_noNan = df.ffill()
    df_resampled = df_noNan.resample(datetime).ffill()
    df_resampled = df_resampled.replace('---', 0)
    return df_resampled
    
def checkFileInFolder(folder, file):
    if file in os.listdir(folder):
        return True
    return False

def gotAnomalies(df:pd.DataFrame):
    unique = df['Anomalia #1'].unique()
    b = False
    for v in unique:
        if v == 0 or v == np.nan:
            b = False
        else:
            b = True
            break
    return b, unique

def getResampledDf(df,file_path,seconds = '30s'):
    if not checkFileInFolder(folder ='last12months/12months',file=file_path):
        df_sorted = sortDf(df)
        df_sorted = cleanCSV(df_sorted)
        df_resampled = resampleDf(df_sorted,'30s')
        df_resampled.to_csv('last12months/12months/' + file_path)
        return df_sorted, df_resampled
    else:
        df_resampled = pd.read_csv('last12months/12months/'+ file_path)
        return None, df_resampled
    

def getDfWithoutNan(df):
    return df.dropna()

if __name__ == '__main__':

    parameter_list = sys.argv


    if '--file1'in parameter_list:
        file1 = os.path.abspath(sys.argv[parameter_list.index("--file1")+1])
    else:
        print("Mandatory parameter --file1 not found please check input parameters")
        sys.exit()

    df = pd.read_csv(file1)
    df['dt'] = pd.to_datetime(df['TIMESTAMP'],unit='ms')
    # df['Anomalia #1'] = df['Anomalia #1'].replace(np.nan,0)
    df_sorted, df_resampled = getResampledDf(df,'220330932_resampled30s.csv','30s')
    # df_resampled['Anomalia #1'] = df_resampled['Anomalia #1'].replace(np.nan,0)

    if gotAnomalies(df_resampled):
        print("Anomalies' values: ",df_resampled['Anomalia #1'].unique())

    dfWithNoNan = getDfWithoutNan(df_resampled)

    print('The shape of the dataframe with no NaN values is: ',dfWithNoNan.shape)
    if dfWithNoNan.shape[0] > 0:
        dfWithNoNan.to_csv('last12months/12months/220330932_resampled30s_noNan.csv')


    