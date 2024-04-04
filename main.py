import sqlite3
import pandas as pd

# Connect to the SQLite database
try:
    conn = sqlite3.connect('Data Engineer_ETL Assignment.db')
    cursor = conn.cursor()

    # Query to get all tables in the database
    sql_query = """
    SELECT name FROM sqlite_master WHERE type='table'
    """

    # Execute the SQL query
    cursor.execute(sql_query)

    # Fetch the results
    tables = cursor.fetchall()

    # Print the list of tables
    print("Tables in the database:")
    for table in tables:
        print(table[0])


    # SQL solution
    # Query to extract total quantities of each item bought per customer aged 18-35
    sql_query = """
    SELECT c.customer_id, c.age, i.item_name, COALESCE(SUM(o.quantity), 0) AS quantity
    FROM customers c
    JOIN sales s ON c.customer_id = s.customer_id
    JOIN orders o ON s.sales_id = o.sales_id
    JOIN items i ON o.item_id = i.item_id
    WHERE c.age BETWEEN 18 AND 35
    GROUP BY c.customer_id, i.item_name
    HAVING quantity > 0
    """

    # Execute the SQL query
    cursor.execute(sql_query)

    # Fetch the results
    results = cursor.fetchall()

    # Store the results in a CSV file
    with open('output_sql.csv', 'w') as file:
        file.write("Customer;Age;Item;Quantity\n")
        for row in results:
            file.write(";".join(map(str, row)) + "\n")

    # Execute the SQL query and fetch the results
    sql_results = pd.read_sql_query(sql_query, conn)

    # Pandas solution
    # Filter customers aged 18-35
    customers_filtered = sql_results[sql_results['age'].between(18, 35)]

    # Store the DataFrame to a CSV file
    customers_filtered.to_csv('output.csv', index=False, sep=';')

except sqlite3.Error as e:
    print("SQLite error:", e)

except Exception as e:
    print("Error:", e)

finally:
    # Close the database connection
    if conn:
        conn.close()
