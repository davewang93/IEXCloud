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
tickerSOI = os.path.join(directory, 'USBasketAUD.csv')
datesList = os.path.join(directory, 'dateslist.csv')
tablename = "audusdusdbasketprice"
exchange = ""


tickers = pd.read_csv(tickerSOI, engine='python')
days = pd.read_csv(datesList, engine='python')
divider=  len(tickers.index)

#this function creates the sum value of all the prices for a specific date for all the securities in the basket
#returns this sum value so it can then be divided to find the average
def sum_price(date, tickers):

    basketsum = 0
    start =  date

    for index,row in tickers.iterrows():
        #remember to toggle exchange
        symbol = row['Ticker'] + exchange
        df = get_historical_data(symbol, start, token = key, close_only=True, output_format='pandas')
        print(df)
        #this allows us to skip days that happen to fall on a weekend or holiday by mistake
        if df.size > 1:
            price = df['close'][0]
            print (symbol + " " + str(price))
            basketsum = basketsum + price

    return basketsum

#this is the main call of the function, where we run the function for every date we want (using csv for initializer)
#we then divide the return of the funtion to generate an average, create a df for that and push it to our sql table
for index,row in days.iterrows():

    date = start = row['Date']
    basketsum = sum_price(date, tickers)
    basketprice = basketsum/divider
    #print(date)
    datesql = datetime.strptime(date, '%m/%d/%Y')
    
    price = pd.DataFrame([[datesql, basketprice]] , columns = ['Date', 'Price'])

    if price['Price'][0] != 0:
        price.to_sql(tablename, engine, if_exists='append')

    print(price)









