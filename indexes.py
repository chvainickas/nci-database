import mysql.connector as mysqlc

config = {
      'user': 'root',
      'password': 'your_new_password', # you'll need to select a password that works for your mySQL database
      'host': 'localhost',
      'database': 'test', # ensure your database has the correct name
      'raise_on_warnings': True
    }
connection = mysqlc.connect(**config)
cursor = connection.cursor()

## Indexes
# Profitability Index
cursor.execute('''
       CREATE INDEX market_price_index
   ON dim_market_price (price_per_kg DESC);
   ''')

cursor.execute('''
        CREATE INDEX idx_spring ON dim_date(date)
        WHERE date BETWEEN '2023-02-01' AND '2023-04-30';
    ''')

cursor.execute('''
        CREATE INDEX idx_summer ON dim_date(date)
        WHERE date BETWEEN '2023-06-01' AND '2023-07-31'
    ''')

cursor.execute('''
        CREATE INDEX idx_autumn ON dim_date(date)
        WHERE date BETWEEN '2023-08-01' AND '2023-10-31'
    ''')
cursor.execute('''
    CREATE INDEX idx_winter ON dim_date(date)
    WHERE date >= '2023-11-01' OR date < '2023-02-01'
    ''')

connection.commit()
connection.close()