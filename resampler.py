import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import os
import sys

def cleanCSV(df):
  arr_col = []
  for col in df.columns:
    if df[col].isna().all():
        print("La colonna", col, "contiene solo valori NaN.")
        arr_col.append(col)
  df_cleaned = df.drop(columns=arr_col)
  df_cleaned = df_cleaned.replace('---', 0)
  return df_cleaned

def sortDf(df):
    if 'TIMESTAMP' in df.columns:
        df = df.sort_values('TIMESTAMP',ascending=True)
        df.index = range(len(df))
    else:
        print("La colonna TIMESTAMP non è presente nel dataframe.")
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

def getUniques(df):
    cols = df.columns
    dict_unique = dict.fromkeys(cols,None)
    for col in cols:
        dict_unique[col] = df[col].unique()
    return dict_unique

def getBooelan(dict):
    arr = []
    for key, value in dict.items():
        if len(value) < 4:
            b = True
            for v in value:
                # print(v,pd.isna(v))
                if pd.isna(v)==False:
                    if int(v) != 0 and int(v) != 1:
                        # print('ciao')
                        b = False
                        break
            # print(value,b)
            if b:
                arr.append(key)
    return arr

def printUniques(dict,tp):
    booleans = getBooelan(dict)
    for key, value in dict.items():
        counts = []
        if key in booleans and tp ==1:
            print(key, value)
        elif tp==2 and len(value) > 4 :
            print(key, value[0:5])

def countOccurences(df,dict):
    booleans = getBooelan(dict)
    counters = dict.fromkeys(booleans,None)
    for key, value in dict.items():
        if key in booleans:
            value = value[1:]
            counts = dict.fromkeys(value,None)
            for v in value:
                if not pd.isna(v):
                    counts[v]= df[key].value_counts()[v]
            counters[key] = counts
            # print(key, counts, max(counts))
    return counters
            
def getMax(dict):
    maxs = dict.fromkeys(dict.keys(),None)
    for key, value in dict.items():
        # print(key, value)
        for k, v in value.items():
            if v == max(value.values()):
                maxs[key] = k
    return maxs

def plotColumn(df,col):
    if col not in df.columns:
        print("La colonna", col, "non è presente nel dataframe.")
        return None
    df_toPlot = df[col]
    plt.plot(df_toPlot)
    try: 
        plt.ylim(0, max(df_toPlot.unique())+1)
    except:
        pass

    plt.show()

def reversedMax(dizionario):
    for key, value in dizionario.items():
        if value == 1:
            dizionario[key] = 0
        else:
            dizionario[key] = 1
    return dizionario

def getGeneralBooleans(df1,df2,df3):
    b1 = getBooelan(getUniques(df1))
    b2 = getBooelan(getUniques(df2))
    b3 = getBooelan(getUniques(df3))
    intersection = list(set(b1) & set(b2) & set(b3))
    intersection.sort()
    return intersection

def fillBooleans(df,booleans):
    counters = countOccurences(df,getUniques(df))
    massimi = getMax(counters)
    r_max = reversedMax(massimi)
    for key in booleans:
        if key != 'Modalità Estate/Inverno (solo scrittura)':        
            df[key] = df[key].replace(np.nan,r_max[key])
    return df

def fillOthers(df,sample):
    col_s = sample.index
    booleani = getBooelan(getUniques(df))
    for col in df.columns:
        # print(col)
        if col not in booleani and col in col_s:
            # print('in')
            df[col] = df[col].replace(np.nan,sample[col])
    return df

def getResampledDf(df,file_path):
    if not checkFileInFolder(folder ='last12months/12months',file=file_path):
        df_sorted = sortDf(df)
        df_sorted = cleanCSV(df_sorted)
        df_resampled = resampleDf(df_sorted,'30s')
        df_resampled.to_csv('last12months/12months/' + file_path)
        return df_sorted, df_resampled
    else:
        df_resampled = pd.read_csv('last12months/12months/'+ file_path)
        return None, df_resampled

if __name__ == '__main__':
    file1 = sys.argv[1]
    file2 = sys.argv[2]
    file3 = sys.argv[3]

    df = pd.read_csv(file1)  
    df2 = pd.read_csv(file2)
    df3 = pd.read_csv(file3)

    df['dt'] = pd.to_datetime(df['TIMESTAMP'],unit='ms')
    df2['dt'] = pd.to_datetime(df2['TIMESTAMP'],unit='ms')
    df3['dt'] = pd.to_datetime(df3['TIMESTAMP'],unit='ms')

    df_sorted, df_resampled = getResampledDf(df,'204300479_resampled30s.csv')
    df2_sorted, df2_resampled = getResampledDf(df2,'220330932_resampled30s.csv')
    df3_sorted, df3_resampled = getResampledDf(df3,'224228487_resampled30s.csv')
    

    if 'last_20Row_204300479.csv' in os.listdir('last12months/12months'):
        df1_last20 = pd.read_csv('last12months/12months/last_20Row_204300479.csv')

    if 'd_test_filled.csv' in os.listdir('last12months/12months'):
        d_test_filled = pd.read_csv('last12months/12months/d_test_filled.csv')
    else:
        print('Filling....')
        d1 = pd.read_csv('last12months/20230721-153054/204300479_LUNA IN PLUS AIR.csv') 
        d1 = cleanCSV(d1)
        d1['dt'] = pd.to_datetime(d1['TIMESTAMP'],unit='ms')
        d1_sorted = sortDf(d1)
        d1_resampled = resampleDf(d1_sorted,'30s')
        d1_resampled = d1_resampled.replace('---', 0)
        if 'd1.csv' not in os.listdir('last12months'):
            d1_resampled.tail(50).to_csv('last12months/d1.csv')
        if 'df1.csv' not in os.listdir('last12months'):
            df_resampled.head(50).to_csv('last12months/df1.csv')


        generalBooleans = getGeneralBooleans(df_resampled,df2_resampled,df3_resampled)
        d_test_fB = fillBooleans(df_resampled,generalBooleans)
        d_campione = d1_resampled.iloc[-1]
        d_test_filled = fillOthers(d_test_fB,d_campione)
        d_test_filled = cleanCSV(d_test_filled)

        d_test_filled.to_csv('last12months/12months/d_test_filled.csv')
        print('Done!')
        d_finale20 = df_resampled.head(20)
        d_finale20.to_csv('last12months/12months/last_20Row_204300479.csv') 

        if 'last_20Row_204300479.xlsx' not in os.listdir('last12months/12months'):
            df1_last20.to_excel('last12months/12months/last_20Row_204300479.xlsx')