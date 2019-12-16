import pandas as pd
import numpy as np
#import psycopg2
import sqlalchemy as db
import os
import glob

#func for list file on my folder dataset/
def list_file():
    path = 'uploadtodb/'
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(file))
    return files, path

#secondary func for read the dataset only
def list_df():
    path = 'uploadtodb/'
    df_lst = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                df_lst.append(os.path.join(file).split(".")[0])
    return df_lst

#for listing unique in list
def unique(list1): 
  
    # intilize a null list 
    unique_list = [] 
      
    # traverse for all elements 
    for x in list1: 
        # check if exists in unique_list or not 
        if x not in unique_list: 
            unique_list.append(x) 
    # print list 
    for x in unique_list: 
        print (x,)
        
#uhm i have idea to soplitt the time for eazying the analitic later. aha make it with function :)
def split_time(cols,colsndate,colsntime,dataframes):
    date = []
    time = []
    dfs = dataframes
    for row in dfs['%s' % cols]:
        try:
            date.append(row.split('T')[0])
            time.append(row.split('T')[1])
        except:
            date.append(np.NaN)
            time.append(np.NaN)

    dfs['%s' % colsndate] = date
    dfs['%s' % colsntime] = time
    
def fixLatLon(dframe, strof_latlon, strof_latname, strof_lonname):
    dframe[strof_latlon]= dframe[strof_latlon].astype(str)
    # Create two lists for the loop results to be placed
    lat = []
    lng = []

    # For each row in a varible,
    for row in dframe[strof_latlon]:
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
    
    # Create two new columns from lat and lon
    dframe[strof_lonname] = lat
    dframe[strof_latname] = lng

    lat2 = []
    lng2 = []

    for row in dframe[strof_lonname]:
        lng2.append(row.split(']')[0])

    for row in dframe[strof_latname]:
        lat2.append(row.split('[')[1])

    dframe[strof_lonname] = lat2
    dframe[strof_latname] = lng2
    
    #fix the location colums 
    dframe[strof_latlon] = dframe[[strof_latname, strof_lonname]].apply(lambda x: ','.join(x), axis = 1)
    return dframe