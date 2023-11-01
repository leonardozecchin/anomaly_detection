import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import os
import sys

if __name__ == '__main__':

    parameter_list = sys.argv


    if '--file1'in parameter_list:
        file1 = os.path.abspath(sys.argv[parameter_list.index("--file1")+1])
    else:
        print("Mandatory parameter --file1 not found please check input parameters")
        sys.exit()

    df = pd.read_csv(file1)

    dfWithAnomalies = df[df['Anomalia #1'] != 0]

    df['Normal/Attack'] = df['Anomalia #1'].apply(lambda x: 'Attack' if x != 0 else 'Normal')

    dfA = df.iloc[0:2000].reset_index(drop=True)
    df = df.iloc[2000:].reset_index(drop=True)
    dfN = dfA.loc[dfA['Normal/Attack'] == 'Normal'].reset_index(drop=True)

    df.to_csv('data/baxi/BAXI_220330932_Normal.csv')
    dfA.to_csv('data/baxi/BAXI_220330932_Attack.csv')