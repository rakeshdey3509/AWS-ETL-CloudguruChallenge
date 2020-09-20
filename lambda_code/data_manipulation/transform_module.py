import json
import psycopg2
import pandas as pd
import numpy as np
import requests
import sys
import boto3 



class dataTypeMistmatch_exception(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg
        sys.exit(1)

def date_convertion(df):
    try:
        pd.to_datetime(df['date'], format='%Y-%m-%d', errors='raise')
        #df['date']=pd.to_datetime(df['date'], format='%Y-%m-%d')
        df['date']=pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
        df=df[df.date.notnull()]
        return df
    except ValueError:
        print("error in object to date covertion")
        exit(1)

def data_type_check(df,cols):
    ab=list(df.columns)
    if not cols==ab:
        raise dataTypeMistmatch_exception("columns not matching")
    else:
        print("columns are matching")
    
    if 'cases' in ab:
        if df.dtypes['cases']!=np.int64:
            raise dataTypeMistmatch_exception("case columns not integer")
        else:
            print("case columns integer")
    if 'deaths' in ab:
        if df.dtypes['deaths']!=np.int64:
            raise dataTypeMistmatch_exception("deaths columns not integer")
        else:
            print("deaths columns integer")
    if 'Recovered' in ab:
        if df.dtypes['Recovered']!=np.int64:
            raise dataTypeMistmatch_exception("recovery columns not integer")
        else:
            print("recovery columns integer")


def merging_two_dataset(df_nyt,df_hopkins,join_type,join_id):
    try:
        df_final=pd.merge(df_nyt,df_hopkins,on=join_id,how=join_type)
        return df_final
    except Exception as e:
        print("merging two dataset is failed" + str(e))
        sys.exit(1)