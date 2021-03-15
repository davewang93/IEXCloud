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

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database
)

#connect to db using sqlalchemy
engine = create_engine(engine)

#this is the iexfinance client
#v1 is live
#iexcloud-sandbox is sandbox
#key setting is to flip between secret key and test key
os.environ['IEX_API_VERSION'] = 'v1'
key = secretkey

#load SOI files and create useful vars
tickerSOI = os.path.join(directory, 'USBasketCAD.csv')
tablename = "usdcadusdbasketprice"


tickers = pd.read_csv(tickerSOI, engine='python')
divider=  len(tickers.index)



# this set of commands pulls the last date from the table and created a DF of date ranges to pass to our update function
my_cursor = mydb.cursor()
my_cursor.execute("SELECT DATE FROM " + tablename + " ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
#convert the str above to datetime in format below
start = datetime.strptime(LastDate, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)
end = datetime.today() 
#pd.bdate_range creates the df with the date ranges we want, then the for loop converts the values to the appropraite format to match with the initializer
datedfRAW = [d.strftime('%m/%d/%Y') for d in pd.bdate_range(start, end)]
#creates final df that will be passed to function
datedf = pd.DataFrame(datedfRAW,columns=['Date'])

#for doc on below, see the initializer file
def sum_price(date, tickers):
    basketsum = 0
    start =  date

    for index,row in tickers.iterrows():

        symbol = row['Ticker']
        #print(symbol + " " + start)
        df = get_historical_data(symbol, start, token = key, close_only=True, output_format='pandas')
        #print(df['close'][0])

        if df.size > 1:
            price = df['close'][0]
            print (symbol + " " + str(price))
            basketsum = basketsum + price

    return basketsum

#for doc on below, see the initializer file
for index,row in datedf.iterrows():

    date = start = row['Date']

    basketsum = sum_price(date ,tickers)

    basketprice = basketsum/divider
    
    #print(date)
    datesql = datetime.strptime(date, '%m/%d/%Y')
    
    price = pd.DataFrame([[datesql, basketprice]] , columns = ['Date', 'Price'])

    if price['Price'][0] != 0:
        price.to_sql(tablename, engine, if_exists='append')

    print(price)







