'''
This script loads the provided json raw data in the database. You can find the files under src/log_files

Heads up! Data can be re-imported at any moment.
'''
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine



def createdb1():
    #Create the first table 
    conn = psycopg2.connect(
    database="postgres", user='postgres', password='sp_technical_test', host='localhost', port= '5432', options='-c search_path=sp_technical_test'
    )
    cursor = conn.cursor()
    #sql code that is going to execute with cursor
    sql = '''
    CREATE TABLE IF NOT EXISTS  log_monetization_transaction(
    index integer ,
    datetime timestamp,
    game_basic_level smallint,
    ip_country char(2),
    order_amount_gross double precision,
    order_payment_provider varchar(16),
    order_transaction_id varchar(255) ,
    platform varchar(8),
    user_id varchar(64) ,
    version varchar(32),
    app_code varchar(10),
    app_name varchar (64),
    currency varchar(10)
    )
        '''
    cursor.execute(sql)
    conn.commit()
    conn.close()


def createdb2():
    #Create the second database
    conn = psycopg2.connect(
    database="postgres", user='postgres', password='sp_technical_test', host='sp_db', port= '5432', options='-c search_path=sp_technical_test',
    )
    cursor = conn.cursor()  
    #Introduce sql query   
    sql = "CREATE TABLE IF NOT EXISTS log_user_register(index integer,client_mobile_device varchar (64),client_mobile_os varchar(64),datetime timestamp,ip_country char(2),	platform varchar(8),	user_id varchar(64)   ,	version varchar(32),	app_code varchar(10),app_name varchar(64)   )"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def insertdb(df,table):
    # First we truncate the table because this json can be change in the future and don't want to load same thing 2 time
    #Furthermore, we are going to have a table with the information of json
    truncate_table(table)
    #Conection define schema
    dbschema='sp_technical_test'
    connections =   'postgresql://postgres:sp_technical_test@sp_db:5432/postgres'

    db = create_engine( connections,    connect_args={'options': '-csearch_path={}'.format(dbschema)}) 
    conn = db.connect()

    conn1 = psycopg2.connect(
    database="postgres", user='postgres', password='sp_technical_test', host='sp_db', port= '5432', options='-c search_path=sp_technical_test'
    )

    cursor = conn1.cursor()
    #Creating table and insert if table exist 
    df.to_sql(table, conn, if_exists='append')
    conn1.close()

def truncate_table(table):
    #With this function it is going to truncate the table
    conn1 = psycopg2.connect(
    database="postgres", user='postgres', password='sp_technical_test', host='sp_db', port= '5432', options='-c search_path=sp_technical_test',
    )
    cursor = conn1.cursor()
    cursor.execute("truncate table " + table)
    conn1.commit()
    conn1.close()




#Firt thing It is going to create the tables that is going to load the data
createdb1()
createdb2()

#Later on load data in data frma 
with open('src/log_files/log_user_register.log') as user_file:
  file1 = user_file.read()
df = pd.read_json(file1,lines=True)

with open('src/log_files/log_monetization_transaction.log') as user_file:
  file2 = user_file.read()
df2= pd.read_json(file2,lines=True)


#Finally is going to insert the data frame inside the tables created before

insertdb(df,'log_user_register')
insertdb(df2,'log_monetization_transaction')

#In this tables we are going to have the raw data and original information
