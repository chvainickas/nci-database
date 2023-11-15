import mysql.connector as mysqlc

config = {
      'user': 'root',
      'password': 'your_new_password', # you'll need to select a password that works for your mySQL database
      'host': 'localhost',
      'database': 'agriculture_sales', # ensure your database has the correct name
      'raise_on_warnings': True
    }
connection = mysqlc.connect(**config)
cursor = connection.cursor()

cursor.execute('''
       CREATE INDEX market_price_index
   ON dim_market_price (price_per_kg DESC);
   ''')

def create_index_if_not_exists(cursor, index_name, table_name, columns, included_columns, condition):
    # Check if the index already exists
    cursor.execute("USE agriculture_sales;")
    cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name = '{index_name}'")
    result = cursor.fetchone()

    if not result:
        # Add a virtual column with the condition
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {index_name} INT GENERATED ALWAYS AS (CASE WHEN {condition} THEN 1 ELSE NULL END) STORED")
        
        # Create a non-clustered index on the specified columns
        cursor.execute(f"CREATE INDEX {index_name} ON {table_name} ({columns[0]})")

        # Create a unique constraint based on the virtual column
        included_columns_str = ', '.join(included_columns) if included_columns else ''

        cursor.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT {index_name}_constraint UNIQUE ({index_name}, {columns[0]}, {included_columns_str}")



create_index_if_not_exists(cursor, 'idx_spring', 'dim_date', ['date'], [], "date BETWEEN '2023-02-01' AND '2023-04-30'")
create_index_if_not_exists(cursor, 'idx_summer', 'dim_date', ['date'], [], "date BETWEEN '2023-06-01' AND '2023-07-31'")
create_index_if_not_exists(cursor, 'idx_autumn', 'dim_date', ['date'], [], "date BETWEEN '2023-08-01' AND '2023-10-31'")
create_index_if_not_exists(cursor, 'idx_winter', 'dim_date', ['date'], [], "date >= '2023-11-01' OR date < '2023-02-01'")
connection.commit()
connection.close()