import mysql.connector
from datetime import datetime
import psycopg2
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
import math
from datetime import timedelta
# import pymysql
# import pymysql.cursors
import osmnx as ox
from leuvenmapmatching.matcher.distance import DistanceMatcher
from leuvenmapmatching.map.inmem import InMemMap
import requests
import folium



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
cursor.execute("SELECT DISTINCT thing_id FROM trace_week where thing_id < 700")
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
	thing_id = %s AND date_id > '{last_date[0][0]}' and date_id < '4' 

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
        time.sleep(0.01)
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
                cursor.execute('SELECT AVG(speed), MAX(speed) FROM trace_week WHERE thing_id = %s AND trace_date >= %s AND trace_date <= %s AND max_speed > 0' , (thing_id, start_trace_date, end_date))
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




# # Insert the rows into the PostgreSQL table
# for journey in all_journeys:
#     insert_query = """
#     INSERT INTO journey (journey_id, thing_id, traveled_distance, idle_time, active_time, avg_speed, max_speed, date_id)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
#     """
#     cursor2.execute(insert_query, journey)




# fixx paths 

def haversine_distance(point1, point2):
    lat1, lon1 = point1
    lat2, lon2 = point2
    R = 6371  # Earth radius in kilometers

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def filter_dense_clusters(gps_traces, min_distance=0.01):
    filtered_traces = [gps_traces[0]]
    for point in gps_traces[1:]:
        if haversine_distance(filtered_traces[-1], point) >= min_distance:
            filtered_traces.append(point)
    return filtered_traces

def filter_large_jumps(gps_traces, max_jump_distance=1.0):
    filtered_traces = [gps_traces[0]]
    for point in gps_traces[1:]:
        if haversine_distance(filtered_traces[-1], point) <= max_jump_distance:
            filtered_traces.append(point)
        else:
            print(f"Large jump detected and filtered: {filtered_traces[-1]} -> {point}")
    return filtered_traces

def parse_path_string(path_string):
    points = path_string.split(';')
    gps_traces = []
    for point in points:
        lat, lon = map(float, point.split(','))
        gps_traces.append((lat, lon))
    return gps_traces

def map_matching_graph2(gps_traces):
    # Filter out dense clusters and large jumps
    filtered_traces = filter_dense_clusters(gps_traces)
    filtered_traces = filter_large_jumps(filtered_traces)
    
    gpx_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
    gpx_content += '<gpx version="1.1" creator="GraphHopper">\n<trk><name>GPS Data</name><trkseg>\n'
    for point in filtered_traces:
        gpx_content += f'    <trkpt lat="{point[0]}" lon="{point[1]}"></trkpt>\n'
    gpx_content += '</trkseg></trk></gpx>'

    url = 'http://localhost:8989/match?profile=car&locale=en&points_encoded=false'
    headers = {'Content-Type': 'application/xml'}
    
    print("GPX Content being sent:\n", gpx_content)

    response = requests.post(url, data=gpx_content, headers=headers)

    if response.status_code != 200:
        print("Request data:", gpx_content)
        print("Response content:", response.content)
        raise RuntimeError(f"GraphHopper API request failed with status code {response.status_code}")

    data = response.json()

    if 'paths' not in data or not data['paths']:
        raise KeyError("'paths' not found in the API response")

    route = data['paths'][0]['points']['coordinates']

    route = [[point[1], point[0]] for point in route]

    return route

def format_matched_path(matched_path):
    return ';'.join([f"{lat},{lon}" for lat, lon in matched_path])

def process_path_string(path_string):
    gps_traces = parse_path_string(path_string)
    try:
        matched_path = map_matching_graph2(gps_traces)
        formatted_path = format_matched_path(matched_path)
        return formatted_path
    except Exception as e:
        # Log detailed information about the problematic GPS traces
        print(f"Error during map matching: {e}")
        print(f"Problematic GPS traces: {gps_traces}")
        raise


# delete  all rows wtih empty path using on cascad
cursor2.execute("DELETE FROM journey_dim WHERE path = ''")
cursor2.execute("DELETE FROM journey WHERE journey_id NOT IN (SELECT journey_id FROM journey_dim)")



print('============================================================')
print('fixing paths')
# apply process_path_string to all paths in the journey_dim table
cursor2.execute(f"""SELECT j.journey_id, path FROM journey_dim d, journey j where date_id > '{last_date[0][0]}' and d.journey_id = d.journey_id """)
rows = cursor2.fetchall()

for row in rows:
    # time.sleep(0.01)
    journey_id = row[0]
    path = row[1]
    try:
        matched_path = process_path_string(path)
        cursor2.execute("UPDATE journey_dim SET path = %s WHERE journey_id = %s", (matched_path, journey_id))
    except Exception as e:
        print(f"Error processing path for journey_id {journey_id} with path {path}: {e}")
        break



# delete all row where path is empty
cursor2.execute("DELETE FROM journey_dim WHERE path = ''")
cursor2.execute("DELETE FROM journey WHERE journey_id NOT IN (SELECT journey_id FROM journey_dim)")



# Commit the transaction
cnx2.commit()


# Close the connection to the MySQL database
cnx.close()


print('============================================================')
print('Paths fixed')