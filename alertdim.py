import psycopg2
import mysql.connector


conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe", # Database name
    user="postgres", # Database user
    password="ryqn" # Database password
)

    # Create a cursor object to interact with the database
cur = conn.cursor()

    # create alert_degree dim table

cur.execute('drop table if exists alert_deg_dim')

cur.execute('drop table if exists alert_fact')



cur.execute('''
        CREATE TABLE IF NOT EXISTS alert_deg_dim (
            alert_degree_id SERIAL PRIMARY KEY,
            alert_degree VARCHAR(50)
        )
    ''')


# create alert fact table
cur.execute('''
    CREATE TABLE IF NOT EXISTS alert_fact (
        alert_id SERIAL PRIMARY KEY,
        thing_id INT ,
        alert_degree_id INT REFERENCES alert_deg_dim(alert_degree_id),
        alert_count INT
        

    )
''')



# connwct to mysql

# Establish a connection to the MySQL server
cnx = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="geopfe"
    )

# Create a cursor object for MySQL
cursor_mysql = cnx.cursor()

query = """
   SELECT * 
from alert_degre


"""

cursor_mysql.execute(query)
rows = cursor_mysql.fetchall()

for row in rows:
    cur.execute("INSERT INTO alert_deg_dim (alert_degree_id, alert_degree) VALUES (%s, %s)", (row[0], row[1]) )

query = """
SELECT thing_id, ad.alert_degre_id ,COUNT(*) from alert_thing ath, alert a,alert_degre ad
WHERE a.alert_id = ath.alert_id and ad.alert_degre_id = a.alert_degre_id
GROUP by thing_id,alert_degre_id

"""

cursor_mysql.execute(query)
rows = cursor_mysql.fetchall()



for row in rows:
    cur.execute("INSERT INTO alert_fact (thing_id, alert_degree_id, alert_count) VALUES (%s, %s,%s)", (row[0], row[1], row[2]))

cnx.commit()


# Commit the changes to the database
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
