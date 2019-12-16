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
import alchemist
from json import dumps
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
import requests
import hivegenerate
import subprocess
import shlex

from hivegenerate import *
from alchemist import *
from apigateway import *
from alerts_cleaner import *
from jams_cleaner import *
from pandas.io.json import json_normalize
from datetime import datetime
from datetime import timedelta


# In[2]:


mydate=['2019-12-07T11:00:00', '2019-12-07T11:00:00']
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

        #alerts_clean.to_sql('alerts', connect_db(),if_exists = 'append')
        jams_clean.to_sql('jams', connect_db(),if_exists = 'append')

        #generating hive query
        hivegenerate = generate_hive_meta(jams_clean)
        subprocess.call(shlex.split('./shell-linux/waze-sqp.sh '+hivegenerate))
        # if want to run this please comand this because the sqoop comand cant run when this file not in ur hadoop hdfs directory

        now += minute
    
    except AttributeError:
        print("Empty Data")
        now += minute
        pass
    
    except: 
        print("Server Down, Trying Again to Connect...")
        time.sleep(5)
        continue


# In[3]:


jams_dict = []
for i in range(len(jams_clean)):
    jams_dict.append(jams_clean.iloc[i].to_dict())


# In[4]:


def connect_kafka_producer(ip,host):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['%s:%s' % (ip,host)], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, value):
    try:
        producer_instance.send(topic_name, value=value)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def filter_key(jamsdata):
    key_used = ['city','nama_kabupaten','speedKMH','real_time', 'delay', 'roadType','weight']
    data =  {k: v for k, v in jamsdata.items() if k in key_used}
    data['real_time'] = dt.datetime.now().isoformat()
    return data

def data_to_push(data):
    HEADER = ['city','nama_kabupaten','speedKMH','real_time', 'delay', 'roadType','weight']
    data['speedKMH'] = round(data['speedKMH'],1)
    data_to_push = [data]
    data_df = pd.DataFrame(data_to_push, columns=HEADER)
    data_json = bytes(data_df.to_json(orient='records'), encoding='utf-8')
    return data_json


# In[ ]:


#if _name_ == '_main_':
rawdata = jams_dict
data_to_stream = rawdata
ip = 'KAFKA IP PRODUCER HERE'
host = 'KAFKA PRODUCER HERE'
api = "Your BI API CODE"
kafka_producer = connect_kafka_producer(ip,host)
i=0
while i < len(data_to_stream):
    try:
        data_to_push_kafka = filter_key(data_to_stream[i])
        data_json = data_to_push(data_to_push_kafka)
        print("JSON dataset", data_json)

        # Push the data to kafka
        publish_message(kafka_producer, 'your kafka topic_name', data_json)
        requests.post(api, data_json)
        time.sleep(2)
        i +=1
    except:
        break

