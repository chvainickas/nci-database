import pandas as pd
import mysql.connector as mysqlc

date_dim = pd.read_csv('time_dim.csv')
sales_dim = pd.read_csv('sales_dim.csv')
market_price_dim = pd.read_csv('market_price_dim.csv')
crop_yield_dim = pd.read_csv('crop_yield_dim.csv')
weather_dim = pd.read_csv('weather_dim.csv')
fact_table = pd.read_csv('fact_agri_prod.csv')

config = {
  'user': 'root',
  'password': 'password', # you'll need to select a password that works for your mySQL database
  'host': '127.0.0.1',
  'database': 'video_store', # ensure your database has the correct name
  'raise_on_warnings': True
}
connection = mysqlc.connect(**config)
mycursor = connection.cursor()

## INSERT STATEMENTS
# Inserting data into time dim table
for row in range(date_dim.shape[0]-1):
    row_data = date_dim.iloc[row]
    # store each column in a separate variable
    date = row_data['date']
    day = row_data['day']
    month = row_data['month']
    year = row_data['year']
    week = row_data['week']

    
    # constructing an sql statement for the database
    sql = "INSERT INTO dim_date (date, day, week, month, year) VALUES (%s, %s, %s, %s)"
    val = (date, day, week, month, year)
    
    try:
        #executing the sql statment and committing it to the DB
        mycursor.execute(sql, val)
    except Exception as e:
        print(e)
    
connection.commit()
