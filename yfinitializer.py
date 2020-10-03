import pandas_datareader as pdr
import quandl as ql
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
from datetime import datetime, timedelta
from configparser import ConfigParser 
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

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy
engine = create_engine(engine)

#load SOI files and create useful vars
tickerSOI = os.path.join(directory, 'JPNBasket.csv')
datesList = os.path.join(directory, 'dateslist.csv')
tablename = "usdjpyjpybasketprice"
exchange = ".T"


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
        #needed to add symbol str thing for tokyo exchange (all numbers)
        symbolstr = row['Ticker'] 
        symbol = str(symbolstr) + exchange

        df = pdr.DataReader(symbol,'yahoo',start)
        #this allows us to skip days that happen to fall on a weekend or holiday by mistake
        if df.size > 1:
            price = df['Adj Close'][0]
            print (symbol + " " + str(price))
            basketsum = basketsum + price

    return basketsum

#this is the main call of the function, where we run the function for every date we want (using csv for initializer)
#we then divide the return of the funtion to generate an average, create a df for that and push it to our sql table
for index,row in days.iterrows():

    date = start = row['Date']
    basketsum = sum_price(date, tickers)
    basketprice = basketsum/divider
    price = pd.DataFrame([[date, basketprice]] , columns = ['Date', 'Price'])

    #this is a check that only allows dfs with data to be pushed to sql, used in conjunction with the if df.size>1 statement in the funtion
    if price['Price'][0] != 0:
        price.to_sql(tablename, engine, if_exists='append')

    print(price)









