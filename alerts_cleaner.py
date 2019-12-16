import pandas as pd
import manipulator
import missingno as masno
import numpy as np

from tqdm import tqdm_notebook
from manipulator import *

def alerts_cleaning(alerts):
    df1 = pd.read_csv(alerts)
    # Create two lists for the loop results to be placed
    lat = []
    lng = []

    # For each row in a varible,
    for row in tqdm_notebook(df1['location']):
        # Try to,
        try:
            # Split the row by comma and append
            # everything before the comma to lat
            lng.append(row.split(',')[0])

            # Split the row by comma and append
            # everything after the comma to lon
            lat.append(row.split(',')[1])

        # But if you get an error
        except:
            # append a missing value to lat
            lng.append(np.NaN)
            # append a missing value to lon
            lat.append(np.NaN)
    
    df1["location"]= df1["location"].astype(str) 
    # Create two new columns from lat and lon
    df1['longitude'] = lat
    df1['latitude'] = lng

    lat2 = []
    lng2 = []

    for row in df1['longitude']:
        lng2.append(row.split(']')[0])

    for row in df1['latitude']:
        lat2.append(row.split('[')[1])

    df1['longitude'] = lat2
    df1['latitude'] = lng2
    
    #fix the location colums 
    df1['location'] = df1[['latitude', 'longitude']].apply(lambda x: ','.join(x), axis = 1)
    df1 = df1.drop(['uuid','country','pubMillis'],axis=1)
    df1 = df1.loc[df1['type'] == 'JAM']
    df_ok = df1.reset_index()
    df_ok = df_ok.drop('index',axis=1)
    
    split_time('created_time', 'create_date', 'create_time', df_ok)
    split_time('end_time', 'ending_date', 'ending_time',df_ok)
    split_time('start_time', 'started_date', 'started_time',df_ok)
    
    alerts_clean = df_ok.drop(['created_time',
                              'end_time',
                              'start_time'],axis=1)
    alerts_clean = alerts_clean.replace(np.nan, 'INFORMATION_MISSING', regex=True)
    return alerts_clean