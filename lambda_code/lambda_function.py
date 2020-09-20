import json
import psycopg2
import pandas as pd
import numpy as np
import requests
import sys
import os
from datetime import datetime
import boto3
import io
from data_manipulation.transform_module import data_type_check
from data_manipulation.transform_module import date_convertion
from data_manipulation.transform_module import merging_two_dataset

s3 = boto3.client('s3')
col_nyt=['date','cases','deaths']
col_hopkins=['date','Recovered']
try:
    config_bucket=os.environ['config_bucket']
    config_file=os.environ['config_file']
    topic_arn=os.environ['sns_topic']
    response = s3.get_object(Bucket = config_bucket, Key = config_file)
    config_dtls = json.loads(response['Body'].read().decode('utf-8'))
    api_url_nyt=config_dtls['api_url_nyt']
    api_url_hopkins=config_dtls['api_url_hopkins']
    host=config_dtls['host']
    port=config_dtls['port']
    database=config_dtls['database']
    user=config_dtls['user']
    password=config_dtls['password']
    table_name=config_dtls['table']
    #topic_arn=config_dtls['TopicArn']
    
except Exception as e: 
    print(str(e))
    sys.exit(1)

#api_url_nyt='https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
#api_url_hopkins='https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'

param_dic = {
    "host"      : host,
    "port"      : port,
    "database"  : database,
    "user"      : user,
    "password"  : password
}

def send_request(body):
    # Create an SNS client
    sns = boto3.client('sns')
    try:
    # Publish a simple message to the specified SNS topic
        sns.publish(
            TopicArn=topic_arn,    
            Message=body,    
        )
    except:
        print("Not able to send message")

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    return conn


def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        #conn.commit()
        return 0
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def database_insert(conn,df_final):
    for i in df_final.index:
        
        query = """INSERT into """+table_name+""" (report_date, cases, deaths,recovered,active_case) values({},{},{},{},{})""".format("'"+str(datetime.date(df_final['date'][i]))+"'", df_final['cases'][i], df_final['deaths'][i],df_final['Recovered'][i],df_final['active_case'][i])
        print(query)
        return_value=single_insert(conn, query)
        if return_value==1:
            sys.exit(1)
    if return_value==0:
        conn.commit()
        send_request("value inserted in table for today -> "+ str(df_final.shape[0]))
    
        
    
def data_load_from_api (api_url):
    try:
        r = requests.get(api_url)  
        df = pd.read_csv(io.StringIO(r.text))
        return df
    except:
        print("not able to load data from api")
        send_request("unable to load data from API ,please look into cloudwatch log")
        sys.exit(1)

def first_time_load(df_nyt,df_hopkins):
    data_type_check(df_nyt,col_nyt)
    data_type_check(df_hopkins,col_hopkins)
    df_nyt=date_convertion(df_nyt)
    df_hopkins=date_convertion(df_hopkins)
    df_final=merging_two_dataset(df_nyt,df_hopkins,'inner','date')
    return df_final
    
def subsequent_load(df_nyt,df_hopkins,max_date):
    data_type_check(df_nyt,col_nyt)
    data_type_check(df_hopkins,col_hopkins)
    df_nyt_temp=date_convertion(df_nyt)
    df_hopkins_temp=date_convertion(df_hopkins)
    
    df_nyt=df_nyt_temp[df_nyt_temp['date']>max_date]
    df_hopkins=df_hopkins_temp[df_hopkins_temp['date']>max_date]
    df_final=merging_two_dataset(df_nyt,df_hopkins,'inner','date')
    return df_final
    

def lambda_handler(event, context):
    #print(api_url_nyt,api_url_hopkins,host,port,database,user,password,table_name,topic_arn)
    #print("********")
    #print(param_dic)
    #sys.exit(1)
    print("data load for nyt api")
    print(api_url_nyt)
    df_nyt=data_load_from_api(api_url_nyt)
    print("data load for hopkins api")
    df_hopkins_tmp=data_load_from_api(api_url_hopkins)
    try:
        df_hopkins_tmp=df_hopkins_tmp[df_hopkins_tmp['Country/Region']=='US'][['Date','Recovered']]
        df_hopkins_tmp.rename(columns={'Date':'date'},inplace=True)
        df_hopkins=df_hopkins_tmp.astype({'Recovered':int})
    except Exception as e:
        print(str(e))
        sys.exit(1)
    
    try:
        conn = connect(param_dic)
        cursor = conn.cursor()
        cursor.execute("select count(*) from "+ table_name)
        res=cursor.fetchall()
        cursor.close()
    except e:
        print("not able to connect")
    print(type(res[0][0]))
    if (res[0][0]==0):
        print("hello first time load")
        df_final=first_time_load(df_nyt,df_hopkins)
        print(df_final.shape[0])
        df_final['active_case']=df_final.apply(lambda row:row['cases']-(row['deaths']+row['Recovered']),axis=1)
        database_insert(conn,df_final)
    else :
        print("not first time")
        cursor = conn.cursor()
        cursor.execute("select max(report_date) from "+ table_name)
        res=cursor.fetchall()[0][0]
        max_date=datetime.strftime(res,"%Y-%m-%d")
        df_final=subsequent_load(df_nyt,df_hopkins,max_date)
        if (df_final.shape[0]>0):
            df_final['active_case']=df_final.apply(lambda row:row['cases']-(row['deaths']+row['Recovered']),axis=1)
            database_insert(conn,df_final)
            
            #print ()
        else:
            print("no value there to insert")
            send_request("No value is there to get inserted for today")
        

