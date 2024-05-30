import mysql.connector
from datetime import datetime
import psycopg2
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time


def get_street_address(p):
    latitude , longitude = p.split(',')
    # geolocator = Nominatim(user_agent="my_app")
    geolocator = Nominatim(user_agent="my_app", timeout=10)  # Increase the timeout to 10 seconds

    try:
        location = geolocator.reverse((latitude, longitude))
        address = location.raw['address']
        return address.get('town'), address.get('state'), address.get('country')

    except GeocoderTimedOut:
        return get_street_address(p)


# Create a connection to the MySQL database
cnx = mysql.connector.connect(user='root', password='', host='localhost', database='geopfe')
cursor = cnx.cursor()

# Create a connection to the PostgreSQL database
cnx2 = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe",
    user="postgres",
    password="ryqn"
)

cursor2 = cnx2.cursor()

# Create the journey_dim table
cursor2.execute('''
    CREATE TABLE IF NOT EXISTS journey_dim (
        journey_id INT PRIMARY KEY,
        start_point VARCHAR(255),
        end_point VARCHAR(255),
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        path TEXT
    )
''')

print('============================================================')

print('journey_dim table created')

#create table journey
cursor2.execute("""
CREATE TABLE IF NOT EXISTS journey (
    id SERIAL PRIMARY KEY,
    journey_id INT,
    thing_id INT,
    traveled_distance FLOAT,
    idle_time FLOAT,
    active_time FLOAT,
    avg_speed FLOAT,
    max_speed FLOAT,
    date_id INT
);
""")

print('============================================================')

print('journey table created')


# Get all distinct thing_ids
cursor2.execute("SELECT MAX(date_id)  FROM journey ")
last_date = cursor2.fetchall()



print('last_date:', last_date[0][0])

# last_date = last_date[0][0]
# last_date = last_date[0][0]


# Get all distinct thing_ids
cursor.execute("SELECT DISTINCT thing_id FROM trace_week where thing_id < 900")
thing_ids = cursor.fetchall()


# get the last journey id from the journy dim table 
cursor2.execute("SELECT MAX(journey_id) FROM journey_dim")
journey_id = cursor2.fetchall()
journey_id = journey_id[0][0]
if journey_id is None:
    journey_id = 0
print('journey_id:', journey_id)

# Initialize an empty list to store all journeys
all_journeys = []

# Loop over all thing_ids
for thing_id_tuple in thing_ids:
    thing_id = thing_id_tuple[0]
    # Get all rows for this thing_id
    cursor.execute(f"""
                   
SELECT
	trace_date,
	thing_id,
	engine_status,
	latitude,
	longitude,
	journey,
	speed,
	date_id
FROM
	trace_week
	JOIN date_dim d ON trace_date_day = full_date
WHERE
	thing_id = %s AND date_id > '{last_date[0][0]}' and date_id <= '4' 

GROUP BY
	thing_id,
	date_id,
	trace_date,
	engine_status,
	latitude,
	longitude,
	journey,
	speed
ORDER BY
	trace_date
                   
                   """, (thing_id,))
    rows = cursor.fetchall()




    print('============================================================')
    print('number of rows:', len(rows))
    print('thing_id:', thing_id)


    # Initialize variables
    start_trace_date = None
    start_journey = None
    path = ''
    idle_time = 0
    active_time = 0

    # Initialize an empty list to store the journeys for this thing_id
    journeys = []

    # Loop over all rows
    for i in range(len(rows)):
        time.sleep(0.001)
        row = rows[i]
        if i > 0 and row[2] == 1 and rows[i-1][2] == 0:
            start_trace_date = row[0]
            start_journey = row[5]
            path = str(row[3]) + ',' + str(row[4])
            idle_time = 0
            active_time = 0
            journey_id += 1
        elif i > 0 and row[2] == 0 and rows[i-1][2] == 1 and start_trace_date is not None:
            date_id = row[7]
            path += ';' + str(row[3]) + ',' + str(row[4])
            end_date = row[0]
            if start_journey is not None:
                active_time = (datetime.strptime(str(end_date), "%Y-%m-%d %H:%M:%S") - datetime.strptime(str(start_trace_date), "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
                cursor.execute('SELECT AVG(speed), MAX(speed) FROM trace_week WHERE thing_id = %s AND trace_date >= %s AND trace_date <= %s', (thing_id, start_trace_date, end_date))
                avg_speed, max_speed = cursor.fetchone()

                start_point = get_street_address(path.split(';')[0])
                end_point = get_street_address(path.split(';')[-1])
                cursor2.execute('INSERT INTO journey_dim (journey_id, start_point, end_point, start_time, end_time, path) VALUES (%s, %s, %s, %s, %s, %s)', (journey_id, start_point, end_point, start_trace_date, end_date, path))

                traveled_distance = row[5] - start_journey
                journeys.append((journey_id, thing_id, traveled_distance, idle_time, active_time , avg_speed, max_speed,date_id))
            start_trace_date = None
            start_journey = None
            path = ''

        if i > 0 and rows[i-1][2] == 1:
            path += ';' + str(row[3]) + ',' + str(row[4])

      
    all_journeys.extend(journeys)

# Print all journeys
# for journey in all_journeys:
#     print(journey)


print('============================================================')

print('journeys inserted into journey table')

print("number of journeys:", len(all_journeys))


# #create table journey
# cursor2.execute("""
# CREATE TABLE IF NOT EXISTS journey (
#     id SERIAL PRIMARY KEY,
#     journey_id INT,
#     thing_id INT,
#     traveled_distance FLOAT,
#     idle_time FLOAT,
#     active_time FLOAT,
#     avg_speed FLOAT,
#     max_speed FLOAT,
#     date_id INT
# );
# """)

# Insert the rows into the PostgreSQL table
for journey in all_journeys:
    insert_query = """
    INSERT INTO journey (journey_id, thing_id, traveled_distance, idle_time, active_time, avg_speed, max_speed, date_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor2.execute(insert_query, journey)




# Commit the transaction
cnx2.commit()


# Close the connection to the MySQL database
cnx.close()