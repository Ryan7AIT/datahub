import psycopg2
from psycopg2 import sql
import random
from datetime import datetime, timedelta

# Connect to your postgres DB
conn = psycopg2.connect("dbname=geopfe user=postgres password=ryqn")

# Open a cursor to perform database operations
cur = conn.cursor()

# Current time
now = datetime.now()

# Generate data for each 10 seconds for an hour
for i in range(6 * 60):  # 6 * 60 = 360, for 360 10-second intervals in an hour
    oil_level = round(random.uniform(2, 5), 2)  # Generate a random oil level
    recorded_at = now + timedelta(seconds=i*10)  # Add i*10 seconds to the current time

    # Prepare the INSERT query
    insert_query = sql.SQL("INSERT INTO oil_level_data (oil_level, recorded_at) VALUES (%s, %s)")

    # Execute the INSERT query
    cur.execute(insert_query, (oil_level, recorded_at))

# Commit the changes
conn.commit()

# Close the cursor and the connection
cur.close()
conn.close()