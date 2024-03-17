import psycopg2

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="your_host",
    port="your_port",
    database="your_database",
    user="your_username",
    password="your_password"
)

print('111')
# Create a cursor object to interact with the database
cur = conn.cursor()

# Perform database operations here

# Close the cursor and connection
cur.close()
conn.close()

# Create the fact table
cur.execute('''
    CREATE TABLE fact_table (
        fact_id SERIAL PRIMARY KEY,
        -- Add your fact table columns here
    )
''')

# Create the first dimension table
cur.execute('''
    CREATE TABLE dim_table1 (
        dim_id SERIAL PRIMARY KEY,
        -- Add your first dimension table columns here
    )
''')

# Create the second dimension table
cur.execute('''
    CREATE TABLE dim_table2 (
        dim_id SERIAL PRIMARY KEY,
        -- Add your second dimension table columns here
    )
''')

# Create the third dimension table
cur.execute('''
    CREATE TABLE dim_table3 (
        dim_id SERIAL PRIMARY KEY,
        -- Add your third dimension table columns here
    )
''')

# Commit the changes to the database
conn.commit()