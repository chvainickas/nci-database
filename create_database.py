import sqlite3

def create_agriculture_sales_database():
    conn = sqlite3.connect('agriculture_sales.db')
    cursor = conn.cursor()

    # Date
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS date_dim (
            date_id INTEGER PRIMARY KEY AUTO_INCREMENT,
            month INTEGER,
            week INTEGER,
            season TEXT,
            year INTEGER
        )
    ''')

    # Sales
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales_dim (
            sales_id INTEGER PRIMARY KEY AUTO_INCREMENT,
            [total_sales(£)] DOUBLE,
            [quantity(per_kg)] INTEGER
        )
    ''')

    # Market Price
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_price_dim (
            price_id INTEGER PRIMARY KEY AUTO_INCREMENT,
            [price(per_kg)] DOUBLE,
            currency_name TEXT,
        )
    ''')

    # Weather
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_dim (
                weather_id INTEGER NOT NULL AUTO_INCREMENT,
                [precipitation(mm)] INTEGER,
                [wind(km/h)] DOUBLE,
                [temperature(C)]) INTEGER;
        )
    ''')

    # Crop Yield
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crop_yield_dim (
                   crop_yield_id INTEGER NOT NULL AUTO_INCREMENT,
                   Quantity INTEGER,
                   Waste INTEGER
    ''')

    

    # Fact Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS agri_fact (
            transaction_id INTEGER PRIMARY KEY AUTO_INCREMENT,
            date_id INTEGER,
            sales_id INTEGER,
            price_id INTEGER,
            weather_id INTEGER,
            crop_yield_id INTEGER,
            FOREIGN KEY (date_id) REFERENCES date_dim (date_id),
            FOREIGN KEY (sales_id) REFERENCES sales_dim (sales_id),
            FOREIGN KEY (price_id) REFERENCES market_price_dim (price_id),
            FOREIGN KEY (weather_id) REFERENCES weather_dim (weather_id),
            FOREIGN KEY (crop_yield_id) REFERENCES crop_yield_dim (crop_yield_id)
        )
    ''')

    # Commit changes and close connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_agriculture_sales_database()
