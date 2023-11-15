import pandas as pd
import mysql.connector as mysqlc

date_dim = pd.read_csv('time_dim.csv')
sales_dim = pd.read_csv('sales_dim.csv')
market_price_dim = pd.read_csv('market_price_dim.csv')
crop_yield_dim = pd.read_csv('crop_yield_dim.csv')
weather_dim = pd.read_csv('weather_dim.csv')
fact_table = pd.read_csv('fact_agri_prod.csv')

# may need to adjust password/user/database as appropriate
config = {
  'user': 'root',
  'password': 'password', # you'll need to select a password that works for your mySQL database
  'host': '127.0.0.1',
  'database': 'agriculture_sales.db', # ensure your database has the correct name
  'raise_on_warnings': True
}
connection = mysqlc.connect(**config)
mycursor = connection.cursor()

## INSERT STATEMENTS
# Inserting data into time dim table
for row in range(date_dim.shape[0]-1):
    row_data = date_dim.iloc[row]
    # store each column in a separate variable
    date = row_data['date'].strftime('%Y-%m-%d')
    day = row_data['day']
    month = row_data['month']
    year = row_data['year']
    week = row_data['week']

    
    # constructing an sql statement for the database
    sql = "INSERT INTO dim_date (date, day, week, month, year) VALUES (%s, %s, %s, %s, %s)"
    val = (date, day, week, month, year)
    
    try:
        #executing the sql statment and committing it to the DB
        mycursor.execute(sql, val)
    except Exception as e:
        print(e)
    
connection.commit()
# print statment to confirm the data was inserted
print(mycursor.rowcount, "record inserted.")
#connection.close()

# Inserting data into sales dim table
for row in range(sales_dim.shape[0]-1):
    row_data = sales_dim.iloc[row]
    total_sales = row_data['total_sales']
    quantity_kg = row_data['quantity_kg']
    
    sql = "INSERT INTO dim_sales (total_sales, quantity_kg) VALUES (%s, %s)"
    val = (total_sales, quantity_kg)
    
    try:
        mycursor.execute(sql, val)
    except Exception as e:
        print(e)
    
connection.commit()
print(mycursor.rowcount, "record inserted.")
#connection.close()

# Inserting data into market price dim table
for row in range(market_price_dim.shape[0]-1):
    row_data = market_price_dim.iloc[row]
    price = row_data['price_per_kg']
    currency_name = row_data['currency_name']
    
    sql = "INSERT INTO dim_market_price (price_per_kg, currency_name) VALUES (%s, %s)"
    val = (market_price_dim, currency_name)
    
    try:
        mycursor.execute(sql, val)
    except Exception as e:
        print(e)
    
connection.commit()
print(mycursor.rowcount, "record inserted.")
#connection.close()

# Inserting data into weather dim table
for row in range(weather_dim.shape[0]-1):
    row_data = weather_dim.iloc[row]
    precipitation = row_data['precipitation_mm']
    wind = row_data['wind_kmh']
    temperature = row_data['temperature_C']

    sql = "INSERT INTO dim_weather (precipitation_mm, wind_kmh, temperature_C) VALUES (%s, %s, %s)"
    val = (precipitation, wind, temperature)
    
    try:
        mycursor.execute(sql, val)
    except Exception as e:
        print(e)
    
connection.commit()
print(mycursor.rowcount, "record inserted.")
#connection.close()

# Inserting data into crop yield table
for row in range(crop_yield_dim.shape[0]-1):
    row_data = crop_yield_dim.iloc[row]
    quantity_cropped = row_data['quantity_cropped']
    waste = row_data['waste']

    sql = "INSERT INTO dim_crop_yield (quantity_cropped, waste) VALUES (%s, %s)"
    val = (quantity_cropped, waste)
    
    try:
        mycursor.execute(sql, val)
    except Exception as e:
        print(e)
    
connection.commit()
print(mycursor.rowcount, "record inserted.")
#connection.close()

# Inserting data into fact table
for row in range(fact_table.shape[0]-1):
    row_data = fact_table.iloc[row]

    product_name = row_data['product_name']
    stock_level_kg = row_data['stock_level_kg']
    
    sql = "INSERT INTO fact_agri_products (product_name, stock_level_kg) VALUES (%s, %s)"
    val = (product_name, stock_level_kg)
    
    try:
        mycursor.execute(sql, val)
    except Exception as e:
        print(e)
    
connection.commit()
print(mycursor.rowcount, "record inserted.")
#connection.close()