#!/usr/bin/env python
# coding: utf-8

# In[1]:


import urllib.request, json 
import pandas as pd
import time
import datetime as dt
import alerts_cleaner
import jams_cleaner
import apigateway
#import tqdm
import alchemyans

from alchemyans import *
from apigateway import *
from alerts_cleaner import *
from jams_cleaner import *
from pandas.io.json import json_normalize


# In[ ]:


mydate=['2019-12-07T11:00:00', '2019-12-12T00:00:00']
fmt = "%Y-%m-%dT%H:%M:%S"
minute = dt.timedelta(minutes=1)
start_time, end_time = [dt.datetime(*(time.strptime(d, fmt)[0:6])) for d in mydate]
now = start_time

while now <= end_time:
    try:
        print('==========================')
        print(now.strftime(fmt))
        print('==========================')

        ed_time = now+dt.timedelta(minutes=1)
        uri = 'http://YOUR WAZE API/?page=1&array_size=800&start_time='+now.strftime(fmt)+'&end_time='+ed_time.strftime(fmt)
        rawdata = getResponse(uri)
        arraylst = ['alerts','jams']
        for key in rawdata.keys():
            if key in arraylst:
                exec('%s = json_normalize(rawdata[key])' % (key))
            else:
                pass

        #temporary csv
        #lerts.to_csv('alerts_tmp.csv',mode = 'w', index=False)
        jams.to_csv('jams_tmp.csv',mode = 'w', index=False)

        #lerts_clean = alerts_cleaning('alerts_tmp.csv')
        jams_clean = jams_cleaning('jams_tmp.csv')

        #lerts_clean.to_sql('alerts', connect_db(),if_exists = 'append')
        jams_clean.to_sql('jams', connect_db(),if_exists = 'append')

        now += minute
    
    except AttributeError:
        print("Empty Data")
        now += minute
        pass
    
    except: 
        print("Server Down, Trying Again to Connect...")
        time.sleep(5)
        continue

