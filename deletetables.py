import psycopg2
import datetime
import mysql.connector

# fill the thing dimmnsion table
#read the last row from the thing_dim table

# Establish a connection to the PostgreSQL server
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe",
    user="postgres",
    password="ryqn"
)

cnx = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="geopfe"
)


# Create a cursor object for PostgreSQL
cursor_postgres = conn.cursor()



# drop thing_dim table
cursor_postgres.execute("DROP TABLE IF EXISTS date_dim")

# drop thing_dim table
# cursor_postgres.execute("DROP TABLE IF EXISTS thing_dim")

# commit 
conn.commit()

