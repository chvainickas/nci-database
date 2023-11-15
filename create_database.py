import sqlite3

def create_agriculture_sales_database():
    conn = sqlite3.connect('agriculture_sales.db')
    cursor = conn.cursor()

    # Tables
    # Date
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
            crop_yield_id INTEGER INTEGER PRIMARY KEY,
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

    
    ## Indexes
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS market_price_index
        ON dim_market_price(price_per_kg DESC)
    ''')

    # Crop Yield Date Index
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_spring ON dim_date(date)
        WHERE date BETWEEN '2023-02-01' AND '2023-04-30'
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_summer ON dim_date(date)
        WHERE date BETWEEN '2023-06-01' AND '2023-07-31'
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_autumn ON dim_date(date)
        WHERE date BETWEEN '2023-08-01' AND '2023-10-31'
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_winter ON dim_date(date)
        WHERE date >= '2023-11-01' OR date < '2023-02-01'
    ''')
        # Commit changes and close connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_agriculture_sales_database()
