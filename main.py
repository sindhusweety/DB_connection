import sqlite3
import pandas as pd

# Connect to the SQLite database
try:
    conn = sqlite3.connect('Data Engineer_ETL Assignment.db')
    cursor = conn.cursor()

    sales = pd.read_sql_query("SELECT * FROM sales", conn)
    orders = pd.read_sql_query("SELECT * FROM `orders`", conn)
    customers = pd.read_sql_query("SELECT * FROM customers", conn)
    items = pd.read_sql_query("SELECT * FROM items", conn)

    # Merge tables
    merged_data = pd.merge(customers, sales, on='customer_id')
    merged_data = pd.merge(merged_data, orders, on='sales_id')
    merged_data = pd.merge(merged_data, items, on='item_id')

    # Filter customers aged 18-35
    customers_filtered = merged_data[merged_data['age'].between(18, 35)]

    # Group by customer_id, age, and item_name, summing up the quantity
    grouped_data = customers_filtered.groupby(['customer_id', 'age', 'item_name']).agg({'quantity': 'sum'}).reset_index()

    # Omit items with no purchase (quantity = 0)
    grouped_data = grouped_data[grouped_data['quantity'] > 0]

    # Write the DataFrame to a CSV file
    grouped_data.to_csv('output.csv', index=False, sep=';')

except sqlite3.Error as e:
    print("SQLite error:", e)

except Exception as e:
    print("Error:", e)

finally:
    # Close the database connection
    if conn:
        conn.close()
