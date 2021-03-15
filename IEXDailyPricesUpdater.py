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
#v1 is live
#iexcloud-sandbox is sandbox
#need to make the switch in the environment variable too
os.environ['IEX_API_VERSION'] = 'v1'
key = secretkey

#load SOI files and create useful vars
tickerSOI = os.path.join(directory, 'DailyPricesList.csv')
#datesList = os.path.join(directory, 'dateslist.csv')

tickers = pd.read_csv(tickerSOI, engine='python')
#days = pd.read_csv(datesList, engine='python')
divider=  len(tickers.index)

for index,row in tickers.iterrows():

    symbol = row['Ticker']
    tablename = row['Table']

    my_cursor = mydb.cursor()
    my_cursor.execute("SELECT DATE FROM " + tablename + " ORDER BY DATE DESC LIMIT 1")
    LastRecord = my_cursor.fetchall()
    LastDate = LastRecord[0][0]
    #convert the str above to datetime in format below
    start = LastDate + timedelta(days=1)
    end = datetime.today()
    df = get_historical_data(symbol, start, end, token = key, close_only=True, output_format='pandas')

    print(df)

    df.to_sql(tablename, engine, if_exists='append')










