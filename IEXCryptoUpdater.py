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
from iexfinance.crypto import get_crypto_price 
import os

#this stuff loads the keys
directory = os.path.dirname(os.path.abspath(__file__))
configfile = os.path.join(directory, 'config.ini')
parser = ConfigParser()
parser.read(configfile)

host = parser.get('iexcloud','host')
user = parser.get('iexcloud','user')
passwd = parser.get('iexcloud','passwd')
database = parser.get('iexcloud','database')

engine = parser.get('engines','iexengine')

#production key
secretkey = parser.get('keys','secretkey')
#sandbox key
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
#this is the pyEX client (iexfinance alt)
#c = pyEX.Client(api_token = secretkey, version = 'v1')

c = pyEX.Client(api_token=testkey, version='sandbox')

df = c.chartDF(symbol, timeframe = '1y', filter='date, symbol, Close')

df.to_sql("test",engine)

print(df.head())
'''

#this is the iexfinance client
#iexcloud-v1 is live
#iexcloud-sandbox is sandbox
#secretkey = live testkey = sandbox
#need to make the switch in the environment variable too
os.environ['IEX_API_VERSION'] = 'iexcloud-v1'
key = secretkey

#load SOI files and create useful vars
tickerSOI = os.path.join(directory, 'cryptouniverse.csv')
#datesList = os.path.join(directory, 'dateslist.csv')
tickers = pd.read_csv(tickerSOI, engine='python')
#days = pd.read_csv(datesList, engine='python')

maintable = pd.DataFrame()

#for each ticker in the file, pulls price data for specified date, and pushes to mysql db under associated table name
for index,row in tickers.iterrows():
    #remember to toggle exchange
    symbol = row['Ticker']
    df = get_crypto_price(symbol, token = key)
    #df['date'] = pd.to_datetime('today').date()
    tickerheader = df.iloc[0]['symbol']
    tickerheader = tickerheader.lower()
    #print(tickerheader)
    df.rename(columns={'price':tickerheader}, inplace = True)
    df = df.drop('symbol',1)
    df.reset_index(drop=True, inplace=True)
    #df = df[['date',tickerheader]]
    #print(df)
    maintable = pd.concat([df, maintable],axis = 1)
    #mastertable = mastertable.append(df, ignore_index = True)
    print("Appended " + tickerheader)

date_col = pd.to_datetime('today').date()
#mastertable['date'] = pd.to_datetime('today').date()
maintable.insert(loc = 0, column = 'date',value = date_col)
#print(maintable)
maintable.to_sql('cryptoprices', engine, index=False, if_exists='append')  






