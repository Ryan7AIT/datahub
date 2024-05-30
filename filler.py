import psycopg2
import random

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe", # Database name
    user="postgres", # Database user
    password="ryqn" # Database password
)

# Create a cursor object to interact with the database
cur = conn.cursor()

# # Step 1: Add the new column 'fuel' to the table 'vehicle_performance'
# cur.execute("ALTER TABLE vehicle_peroformance ADD COLUMN fuel REAL")

# # Step 2: Retrieve all rows to update the new column using ctid
# cur.execute("SELECT ctid, travled_distance FROM vehicle_peroformance")
# rows = cur.fetchall()

# # Step 3: Update each row with the appropriate random 'fuel' value
# for row in rows:
#     ctid = row[0]
#     travled_distance = row[1]
#     if (travled_distance is not None) and (travled_distance > 100):
#         fuel = random.uniform(9, 12)
#     else:
#         fuel = random.uniform(3, 7)
    
#     cur.execute("UPDATE vehicle_peroformance SET fuel = %s WHERE ctid = %s", (fuel, ctid))


# Step 1: Update max_speed if greater than 200
# cur.execute("""
#     UPDATE vehicle_peroformance
#     SET max_speed = 120
#     WHERE max_speed > 200
# """)

# # Step 2: Update avg_speed if greater than 200
# cur.execute("""
#     UPDATE vehicle_peroformance
#     SET avg_speed = 80
#     WHERE avg_speed > 200
# """)



# Step 1: Retrieve all rows to update the date_id column using ctid
cur.execute("SELECT ctid FROM alert_fact")
rows = cur.fetchall()

# Step 2: Update each row with a random date_id value between 1 and 8
for row in rows:
    ctid = row[0]
    date_id = random.randint(1, 8)
    cur.execute("UPDATE alert_fact SET date_id = %s WHERE ctid = %s", (date_id, ctid))

# Commit the changes and close the connection





# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
