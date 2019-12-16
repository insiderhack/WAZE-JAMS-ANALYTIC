import pandas as pd
import manipulator
import missingno as masno
import numpy as np
import ast
import apigateway
import datetime
import sys

from time import sleep
from apigateway import *
from manipulator import *

def jams_cleaning(jams):
    print('jams cleanser start')
    ## data province for gather kabupaten
    province_df = getResponse('http://dev.farizdotid.com/api/daerahindonesia/provinsi/32/kabupaten')
    province_df = apiNormalizer(province_df, 'kabupatens')
    province_df = province_df.drop('id_prov',axis = 1) 
    province_df = province_df.rename(columns = {'id':'kode_kabupaten'})

#     ## external table for postalcode
#     post_df = pd.read_csv('ext_table/postalcode_id.csv')
#     post_df = post_df.drop(['sub_district','id','urban','province_code'],axis=1)
    try:
        df = pd.read_csv(jams)
    
        split_time('created_time', 'create_date', 'create_time', df)
        split_time('end_time', 'ending_date', 'ending_time',df)
        split_time('start_time', 'started_date', 'started_time',df)

        df = df.drop(['created_time',
                                      'end_time',
                                      'start_time', 
                                      'nama_kabupaten'],axis=1)

        jams_clean = df

        jams_clean = jams_clean.drop(['turnType','type','country','uuid'], axis=1)

        pubMillis_lst = jams_clean['pubMillis'].tolist()

        pubmil = []
        for mil in pubMillis_lst:
            pubmil.append(datetime.datetime.fromtimestamp(float(mil)/1000).strftime('%Y-%m-%dT%H:%M:%S'))

        jams_clean['pubMillis'] = pubmil

        line_lst = jams_clean['line'].tolist()
        lines = []
        lines_last = []
        lines_first = []

        for line in range(len(line_lst)):
            pull = ast.literal_eval(jams_clean['line'].iloc[line])
            first = ast.literal_eval(jams_clean['line'].iloc[line])[0]
            last = ast.literal_eval(jams_clean['line'].iloc[line])[-1]
            lines.append(pull)
            lines_first.append(first)
            lines_last.append(last)

        jams_clean['line_first'] = lines_first
        jams_clean['line_last'] = lines_last

        jams_clean["kode_kabupaten"]= jams_clean["kode_kabupaten"].astype(str)
        kodkab_lst = jams_clean['kode_kabupaten'].tolist()
        new_kodkab = []

        for kab in kodkab_lst:
            new_kodkab.append(kab.replace('.',''))

        jams_clean['kode_kabupaten']= new_kodkab
        jams_clean = pd.merge(jams_clean, province_df, how='left', on='kode_kabupaten')
        jams_clean = jams_clean.rename(columns = {'nama':'nama_kabupaten'})

        dfx = fixLatLon(jams_clean, 'line_first', 'linef_latitude', 'linef_longitude')
        dfx = fixLatLon(dfx, 'line_last', 'linel_latitude', 'linel_longitude')

    linef_lst = dfx['line_first'].tolist()
    linel_lst = dfx['line_last'].tolist()

    fulladdr_first = []
    addr_first = []
    for addr in tqdm_notebook(linef_lst):
        while True:
            try:
                x = get_address(addr)
                xx = get_fulladdress(addr)
                addr_first.append(x['address']['road'])
                fulladdr_first.append(xx)
                #print ('OK1')
            except KeyError:
                try:
                    x = get_address(addr)
                    xx = get_fulladdress(addr)
                    addr_first.append(x['address']['village'])
                    fulladdr_first.append(xx)
                    #print ('OK2')
                except:
                    x = get_address(addr)
                    xx = get_fulladdress(addr)
                    addr_first.append('address not found')
                    fulladdr_first.append(xx)
                    #print ('OK3')
                    continue
            except:
                print("access paused")
                sleep(5)
                continue
            break

    dfx['fulladdr_first'] = fulladdr_first
    dfx['startNode'] = addr_first
    dfx['endNode'] = addr_first
    dfx['street'] = addr_first

    fulladdr_last = []
    addr_last = []
    for addr in tqdm_notebook(linef_lst):
        while True:
            try:
                x = get_address(addr)
                xx = get_fulladdress(addr)
                addr_last.append(x['address']['road'])
                fulladdr_last.append(xx)
                #print ('OK1')
            except KeyError:
                try:
                    x = get_address(addr)
                    xx = get_fulladdress(addr)
                    addr_last.append(x['address']['village'])
                    fulladdr_last.append(xx)
                    #print ('OK2')
                except:
                    x = get_address(addr)
                    xx = get_fulladdress(addr)
                    addr_last.append('address not found')
                    fulladdr_last.append(xx)
                    #print ('OK3')
                    continue
            except:
                print("access paused")
                sleep(5)
                continue
            break

    dfx['fulladdr_last'] = fulladdr_last
    dfx['endNode'] = addr_last

    #featured engineering to make wight on the spotted areas
        dfx['weight']=1

        dfx = dfx.replace(np.nan, 'Information_missing', regex=True)
        print('jams cleanser done')
        return dfx
    
    except:
        print('data empty, passing..')
        pass
