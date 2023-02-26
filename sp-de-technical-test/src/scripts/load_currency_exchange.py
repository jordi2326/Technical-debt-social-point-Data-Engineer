'''
This script loads daily currency exchange in the database.

Please use this API: https://exchangeratesapi.io

Heads up! Data can be re-imported at any moment.
'''

import requests
import pandas as pd 
import json
import psycopg2
import datetime
from datetime import datetime
from sqlalchemy import create_engine


start_date = 0
end_date = 0

def create_table ():
    #Create the conection
    conn = psycopg2.connect(
    database="postgres", user='postgres', password='sp_technical_test', host='localhost', port= '5432', options='-c search_path=sp_technical_test',
    )
    cursor = conn.cursor()
    #Define table that it is going to create. Fecha and contry we define as pk
    sql = '''
        CREATE TABLE IF NOT EXISTS  change_rate (
        index integer,
        fecha date  ,
        country varchar(4) ,
        exchange_rate double precision,
        PRIMARY KEY (fecha, country)

            )
        '''
    #execute query
    cursor.execute(sql)
    conn.commit()
    conn.close()

def find_min_data():
    #find min data from log_monetization_transaction 
     conn = psycopg2.connect(
     database="postgres", user='postgres', password='sp_technical_test', host='localhost', port= '5432', options='-c search_path=sp_technical_test',
     )
     cursor = conn.cursor()
    #Define table that it is going to create after 
     sql = "select cast(date(min(datetime)) as text) as fecha from  log_monetization_transaction"
    #execute query
     cursor.execute(sql)
     row = cursor.fetchone() 
     return (row[0])

def find_max_data():
        #find max data from log_monetization_transaction 
     conn = psycopg2.connect(
     database="postgres", user='postgres', password='sp_technical_test', host='localhost', port= '5432', options='-c search_path=sp_technical_test',
     )
     cursor = conn.cursor()
    #Define table that it is going to create after 
     sql = "select cast(date(max(datetime)) as text) as fecha from  log_monetization_transaction"
    #execute query
     cursor.execute(sql)
     row = cursor.fetchone() 
     return (row[0])    


def truncate_table(table):
    conn1 = psycopg2.connect(
    database="postgres", user='postgres', password='sp_technical_test', host='sp_db', port= '5432', options='-c search_path=sp_technical_test',
    )
    cursor = conn1.cursor()
    cursor.execute("truncate table " + table)
    conn1.commit()
    conn1.close()


def insert_values(df,start_date,end_date):
    #This function it is goig to insert the values to the final table of change rate 
    #Always truncate table, every load delete all information about exchange and loading the new data
    truncate_table('change_rate')
    dbschema='sp_technical_test'
    connections =   'postgresql://postgres:sp_technical_test@sp_db:5432/postgres'
  
    db = create_engine( connections,    connect_args={'options': '-csearch_path={}'.format(dbschema)}) 
    conn = db.connect()

    conn1 = psycopg2.connect(
    database="postgres", user='postgres', password='sp_technical_test', host='sp_db', port= '5432', options='-c search_path=sp_technical_test',
    )
    cursor = conn1.cursor()
  
    df.to_sql('change_rate', conn, if_exists='append')
    conn1.close()

def extract_data():
    #Url from api 
    url = "https://api.apilayer.com/exchangerates_data/timeseries?start_date="+start_date+"&end_date="+end_date+"&base=USD"

    payload = {}
    headers= {
    "apikey": "ET5CojqIlqBvwCsQxpnnsJJswdwVW3pu"
    }

    response = requests.request("GET", url, headers=headers, data = payload)

    status_code = response.status_code
    result = response.json()

    #Obtain the date that it is going to use to make the exchange value , value reference USD. The key that i want it is from rates
    diccionario = result['rates']
    my_df = []
    for key in diccionario:
        diccionario2 =diccionario[key]
        #Fecha is going to obtain the data, after that we are going to iterate the key that is fecha to find country and rate
        fecha = key
        for value in diccionario2:
            #Value is the key that refers to country (ISO reference ) and we save as country
            country = value
            #Then diccionario2[value] contain the values from the exchange rate 
            val =diccionario2[value] 
            #create data
            dic_info = {'fecha': fecha, 'country': country, 'exchange_rate': val}
            #add info to later on create data frame 
            my_df.append(dic_info)


    #Creating the dataframe 
    df = pd.DataFrame(my_df)
    return df

#Find minimum value
start_date=find_min_data()
#Find maximum value
end_date=find_max_data()
#Request api using the 2 global variables that it searching before
df = extract_data()
#Create table 
create_table()
#Insert data to table 
insert_values(df,start_date,end_date)