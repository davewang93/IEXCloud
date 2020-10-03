import pandas_datareader as pdr
import quandl as ql
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
from datetime import datetime, timedelta
from configparser import ConfigParser 
import pyEX
from iexfinance.stocks import get_historical_data
import os

directory = os.path.dirname(os.path.abspath(__file__))
configfile = os.path.join(directory, 'config.ini')
parser = ConfigParser()
parser.read(configfile)

host = parser.get('iexcloud','host')
user = parser.get('iexcloud','user')
passwd = parser.get('iexcloud','passwd')
database = parser.get('iexcloud','database')

engine = parser.get('engines','iexengine')

secretkey = parser.get('keys','secretkey')

testkey = parser.get('keys','testkey')

'''
# Create a new DB in mySQL w/ block below
mydb = mysql.connector.connect(
        host = host,
        user = user,
        passwd = passwd,
    )

#create cursor
cursor = mydb.cursor()

#create a db
cursor.execute("CREATE DATABASE iexcloud")
'''

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

'''
#this is the pyEX client
#c = pyEX.Client(api_token = secretkey, version = 'v1')

c = pyEX.Client(api_token=testkey, version='sandbox')

df = c.chartDF(symbol, timeframe = '1y', filter='date, symbol, Close')

df.to_sql("test",engine)

print(df.head())
'''

#this is the iexfinance client
#v1 is live
#iexcloud-sandbox is sandbox

basketfile = os.path.join(directory, 'AUSBasket.csv')
dateslist = os.path.join(directory, 'dateslist.csv')
ticker = pd.read_csv(basketfile, engine='python')
days = pd.read_csv(dateslist, engine='python')

os.environ['IEX_API_VERSION'] = 'iexcloud-sandbox'
key = testkey

for index,row in ticker.iterrows():

    table = row['Table'].lower()
    symbol = row['Ticker'] + "-CT"

    for index,row in days.iterrows():

        start = row['Date']
        #start = datetime(2015, 8, 21)
        #end = datetime.today().strftime('%Y-%m-%d')
        '''
        yesterday = datetime.now() - timedelta(days=1)
        yesterday.strftime('%Y-%m-%d')
        '''
        df = get_historical_data(symbol, start, token = key, close_only=True, output_format='pandas')

        if df.size > 1:
            print(table)
            print(df)
            df.to_sql(table, engine, if_exists='append')
        
        #df.to_csv("test.csv")



#print(end)






