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
tablename = "usdjpyjpybasketprice"
exchange = ".T"

tickers = pd.read_csv(tickerSOI, engine='python')
divider=  len(tickers.index)

# this set of commands pulls the last date from the table and created a DF of date ranges to pass to our update function
my_cursor = mydb.cursor()
my_cursor.execute("SELECT DATE FROM " + tablename + " ORDER BY DATE DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
#convert the str above to datetime in format below
start = datetime.strptime(LastDate, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)
print(start)
end = datetime.today()
#pd.bdate_range creates the df with the date ranges we want, then the for loop converts the values to the appropraite format to match with the initializer
datedfRAW = [d.strftime('%m/%d/%Y') for d in pd.bdate_range(start, end)]
#creates final df that will be passed to function
datedf = pd.DataFrame(datedfRAW,columns=['Date'])

#this function creates the sum value of all the prices for a specific date for all the securities in the basket
#returns this sum value so it can then be divided to find the average
def sum_price(date, tickers):

    basketsum = 0
    start =  date

    for index,row in tickers.iterrows():
        #remember to toggle exchange 
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
for index,row in datedf.iterrows():

    date = start = row['Date']
    basketsum = sum_price(date, tickers)
    basketprice = basketsum/divider
     
    #print(date)
    datesql = datetime.strptime(date, '%m/%d/%Y')
    
    price = pd.DataFrame([[datesql, basketprice]] , columns = ['Date', 'Price'])

    if price['Price'][0] != 0:
        price.to_sql(tablename, engine, if_exists='append')

    print(price)









