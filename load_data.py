import pandas as pd
from sqlalchemy import create_engine

date_dim = pd.read_csv('time_dim.csv')
sales_dim = pd.read_csv('sales_dim.csv')
market_price_dim = pd.read_csv('market_price_dim.csv')
crop_yield_dim = pd.read_csv('crop_yield_dim.csv')
weather_dim = pd.read_csv('weather_dim.csv')
fact_table = pd.read_csv('fact_agri_prod.csv')

username = 'root'
password = 'your_new_password'
host = 'localhost'
database_name = 'agriculture_sales'

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}/{database_name}")

# Inserting DataFrames into corresponding tables
fact_table.to_sql('fact_agri_products', con=engine, index=False, if_exists='replace')
date_dim.to_sql('dim_date', con=engine, index=False, if_exists='replace')
sales_dim.to_sql('dim_sales', con=engine, index=False, if_exists='replace')
market_price_dim.to_sql('dim_market_price', con=engine, index=False, if_exists='replace')
crop_yield_dim.to_sql('dim_crop_yield', con=engine, index=False, if_exists='replace')
weather_dim.to_sql('dim_weather', con=engine, index=False, if_exists='replace')

# Counting the total number of rows inserted
total_rows_inserted = (
    date_dim.shape[0] +
    sales_dim.shape[0] +
    market_price_dim.shape[0] +
    crop_yield_dim.shape[0] +
    weather_dim.shape[0]
)

print(total_rows_inserted, "records inserted.")

# Close the connection
engine.dispose()
