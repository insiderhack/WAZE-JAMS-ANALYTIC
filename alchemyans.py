import pandas as pd
import numpy as np
import powers as ccd
import psycopg2
import sqlalchemy
import manipulator

from manipulator import *
from sqlalchemy import MetaData,create_engine, Column, Integer, String, Sequence, Float,PrimaryKeyConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.sql import *

#connecting to database with this param
DB_USER = ccd.userdb
DB_PASS = ccd.dpass
DB_NAME = ccd.dbs
DB_IP = ccd.dbip
DB_PORT = ccd.dbpt

#func for connect to DB
def connect_db():
    #_load_db_vars()
    # create db create_engine
    databases = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_IP}:{DB_PORT}/{DB_NAME}')
    connection = databases.connect()
    metadata = MetaData(bind=databases)
    return databases

#declare base for sqlalchemy
Base = declarative_base()

#created initied database, its return value integer to limit the database upload each database to posgresql
def construct_dbs(limitbosku):
    engine = connect_db()
    files,path = list_file()
    df_lst = list_df()

    print("Loading Files...")
    print("")

    #reading the datasets
    for f in files:
        exec("%s = pd.read_csv('%s')" % (f.split(".")[0],path+f))

    #limiting the datasets for uploading to postgres
    #limitbosku = 10
    if (limitbosku == 0):
        limitbosku = 1
        for d in df_lst:
            exec("%s = %s.sample(n=%s, random_state=1, replace=False)" % (d, d, limitbosku))
    elif limitbosku in ['unlimited', 'full', 'all', 'kabeh', 'semua']:
        for d in df_lst:
            exec("%s = %s" % (d, d))
    else:
        limitbosku = limitbosku
        for d in df_lst:
            exec("%s = %s.sample(n=%s, random_state=1, replace=False)" % (d, d, limitbosku))

    #Engineer Alchemist GOOOOO!!!
    u = ' uploaded safely ^.^'
    for alch in df_lst:
        exec("%s.to_sql('%s', engine, if_exists='replace', index=False)" % (alch,alch))
        print(alch.split(".")[0]+u)
        
#uploading spatial database using this comand upload_dbs(dataset, limitupload, dbname)
def upload_dbs(dsup, inlimits, namedb):
    engine = connect_db()
    ds_up = dsup.sample(n=inlimits, random_state=1, replace=False)
    ds_up.to_sql(namedb, engine, if_exists='replace', index=False)
    print('database successfully created.')