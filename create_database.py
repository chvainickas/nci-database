import mysql.connector as mysqlc

def create_agriculture_sales_database():
    config = {
      'user': 'root',
      'password': 'your_new_password', # you'll need to select a password that works for your mySQL database
      'host': 'localhost',
      'database': 'agriculture_sales', # ensure your database has the correct name
      'raise_on_warnings': True
    }
    connection = mysqlc.connect(**config)
    cursor = connection.cursor()

    try:
        cursor.execute("SHOW DATABASES LIKE 'test'")
        database_exists = cursor.fetchone()

        if not database_exists:
            # Create the 'test' database if it doesn't exist
            cursor.execute("CREATE DATABASE IF NOT EXISTS test;")
        else:    
            cursor.execute("USE test;")
    
    except mysqlc.Error as err:
        print(f"Error: {err}")

    ##Tables
    #Date
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INTEGER PRIMARY KEY,
            date DATE,
            day INTEGER,
            week INTEGER,
            month INTEGER,
            year INTEGER
        )
    ''')

    # Sales
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_sales (
            sales_id INTEGER PRIMARY KEY,
            total_sales DOUBLE,
            quantity_kg INTEGER
        )
    ''')

    # Market Price
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_market_price (
            price_id INTEGER PRIMARY KEY,
            price_per_kg DOUBLE,
            currency_name VARCHAR(50)
        )
    ''')

    # Weather
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_weather (
            weather_id INTEGER PRIMARY KEY,
            precipitation_mm INTEGER,
            wind_kmh DOUBLE,
            temperature_C INTEGER
        )
    ''')

    # Crop Yield
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_crop_yield (
            crop_yield_id INTEGER PRIMARY KEY,
            quantity_cropped INTEGER,
            waste INTEGER
        )
    ''')

    

    # Fact Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fact_agri_products (
            product_name TEXT,
            stock_level_kg INTEGER,
            date_id INTEGER,
            sales_id INTEGER,
            price_id INTEGER,
            weather_id INTEGER,
            crop_yield_id INTEGER,
            FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
            FOREIGN KEY (sales_id) REFERENCES dim_sales (sales_id),
            FOREIGN KEY (price_id) REFERENCES dim_market_price (price_id),
            FOREIGN KEY (weather_id) REFERENCES dim_weather (weather_id),
            FOREIGN KEY (crop_yield_id) REFERENCES dim_crop_yield (crop_yield_id)
        )
    ''')

        # Commit changes and close connection
    connection.commit()
    connection.close()


if __name__ == "__main__":
    create_agriculture_sales_database()