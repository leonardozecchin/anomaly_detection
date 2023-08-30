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

    df_caldaia = df_sorted[['Caldaia','dt']]
    
    print(df_caldaia.head())