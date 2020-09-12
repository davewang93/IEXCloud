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

parser = ConfigParser()
parser.read('config.ini')

host = parser.get('iexcloud','host')
user = parser.get('iexcloud','user')
passwd = parser.get('iexcloud','passwd')
database = parser.get('iexcloud','database')

engine = parser.get('engines','iexengine')

secretkey = parser.get('keys','secretkey')

testkey = parser.get('keys','testkey')

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

my_cursor = mydb.cursor()

#connect to db using sqlalchemy
engine = create_engine(engine)

ticker = pd.read_csv("CANBasket.csv", engine='python')

os.environ['IEX_API_VERSION'] = 'iexcloud-sandbox'

for index,row in ticker.iterrows():
    #create sql command
    sqlcmd = "SELECT DATE FROM " +row['Table'] + " ORDER BY DATE DESC LIMIT 1"
    #get individual values
    symbol= row['Ticker'] + "-CT"
    table = row['Table'].lower()

    my_cursor.execute(sqlcmd)
    LastRecord = my_cursor.fetchall()
    LastDate = LastRecord[0][0]
    start = LastDate.date() + timedelta(days=1)
    end = datetime.today().strftime('%Y-%m-%d')
    print(start)
    print(end)
    df = get_historical_data(symbol, start, end, token = testkey, close_only=True, output_format='pandas')
    print(df)
    '''
    table = dataseries.reset_index()
    table.columns = ['Date','Value']
    #this line recreates the table without the top row ([0]). This is necessary for some of these 
    # because the query is inclusive of the immediate previous value and i can't bother to figure out why
    table = table.iloc[1:]
    '''
    
    if df.size > 1:
        #print(tableID)
        #print(table)
        df.to_sql(table,engine,if_exists='append')
    