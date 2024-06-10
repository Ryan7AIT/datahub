# create tables 
import psycopg2
import mysql.connector
import datetime


conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe", # Database name
    user="postgres", # Database user
    password="ryqn" # Database password
)

    # Create a cursor object to interact with the database
cur = conn.cursor()

print("=====================================")
print("tables are created successfully")

# extract data from the source database

# alert fact and alert degree dim tables

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

# check if alert_deg_dim it's empty

cur.execute("SELECT COUNT(*) FROM alert_deg_dim")
result = cur.fetchone()
if result[0] == 0:
    for row in rows:
        cur.execute("INSERT INTO alert_deg_dim (alert_degree_id, alert_degree) VALUES (%s, %s)", (row[0], row[1]) )


print("=====================================")
print("alert degree up to date")

query = """
SELECT event_id, event_designation from  event

"""

cursor_mysql.execute(query)
rows = cursor_mysql.fetchall()

cur.execute("SELECT COUNT(*) FROM event_dim")
result = cur.fetchone()
if result[0] == 0:
    for row in rows:
        cur.execute("INSERT INTO event_dim (event_id, event_designation) VALUES (%s, %s)", (row[0], row[1]) )


print("=====================================")
print("alert events is up to date")


if result[0] == 0:
    query = """
    SELECT
        thing_id,
        ad.alert_degre_id,
        ae.event_id,
        p.point_id,
        COUNT(*)
    FROM
        alert_thing ath,
        alert a,
        alert_degre ad,
        alert_event ae,
        alert_point ap,
        point p
    WHERE
        a.alert_id = ath.alert_id
        AND ad.alert_degre_id = a.alert_degre_id
        AND ae.alert_id = a.alert_id
        AND ap.alert_id = a.alert_id
        AND p.point_id = ap.point_id
    GROUP BY
        thing_id,
        alert_degre_id,
        ae.event_id,
        p.point_id

    """

    cursor_mysql.execute(query)
    rows = cursor_mysql.fetchall()



    for row in rows:
        cur.execute("INSERT INTO alert_fact (user_id, type_id, event_id,location_id ,alert_count) VALUES (%s, %s,%s, %s, %s)", (row[0], row[1], row[2], row[3], row[4]))


    print("=====================================")
    print("ALERT data is up to date")




print("=====================================")
print("=====================================")
print('alert date dim')

# Read the last row from the date_dim table
cur.execute("SELECT full_date FROM date_dim ORDER BY date_id DESC LIMIT 1")
#check if the cursor result is not empty


result = cur.fetchone()
#check if the last row is empty

if result is not None:
    last_date_id = result[0]
    print(last_date_id)
else:
    current_date = datetime.date.today().strftime("%Y-%m-%d")
    last_date_id = '2020-01-01'
    print('No data found in df_last_row date dim')

# Create a cursor object for MySQL
cursor_mysql = cnx.cursor()

cursor_mysql.execute(f"""
                     SELECT DISTINCT
                        date_insertion_day AS full_date,
                        YEAR(date_insertion_day) AS year,
                        MONTH(date_insertion_day) AS month,
                        DAY(date_insertion_day) AS day,
                        DATE_FORMAT(date_insertion_day, '%Y-%m') AS month_year,
                        DATE_FORMAT(date_insertion_day, '%M') AS month_name,
                        CONCAT(YEAR(date_insertion_day), '-Q', QUARTER(date_insertion_day)) AS quarter,
                        CASE WHEN DAYOFWEEK(date_insertion_day)
                        IN(1, 7) THEN
                            'Weekend'
                        ELSE
                            'Weekday'
                        END AS day_type,
                        CASE WHEN MONTH(date_insertion_day)
                        IN(1, 2, 12) THEN
                            'Winter'
                        WHEN MONTH(date_insertion_day)
                        IN(3, 4, 5) THEN
                            'Spring'
                        WHEN MONTH(date_insertion_day)
                        IN(6, 7, 8) THEN
                            'Summer'
                        ELSE
                            'Fall'
                        END AS season
                            FROM
                                geopfe.trace_week
                            WHERE
                                date_insertion_day > '{last_date_id}'
                     """)

rows = cursor_mysql.fetchall()




# insert this row into the date_dim table
insert_query = """
INSERT INTO date_dim (full_date, year, month, day, month_year, month_name, quarter, day_type, season)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
cur.executemany(insert_query, rows)


print("=====================================")
print("date dim is loaded successfully")


print("=====================================")
print("insert inot user dim")

query = """
SELECT
	u.id,u.name
FROM
	users u
"""

cursor_mysql.execute(query)
rows = cursor_mysql.fetchall()


# check if user_dim it's empty

cur.execute("SELECT COUNT(*) FROM user_dim")
result = cur.fetchone()
if result[0] == 0:
    for row in rows:
        cur.execute("INSERT INTO user_dim (user_id, user_name) VALUES (%s, %s)", (row[0], row[1]) )


    

print("=====================================")
print('Done with user dim')


print("=====================================")
print("insert into location dim")

query = """
SELECT 
	
	w.wilaya_designation, wilaya_designation
	
FROM
	 alert_point ap, point p, wilaya w, commune c
	 
WHERE ap.point_id = p.point_id
AND p.wilaya_id = w.wilaya_id 
AND p.commune_id = c.commune_id

"""

cursor_mysql.execute(query)
rows = cursor_mysql.fetchall()

# check if location_dim it's empty
cur.execute("SELECT COUNT(*) FROM location_dim")
result = cur.fetchone()
if result[0] == 0:
    for row in rows:
        cur.execute("INSERT INTO location_dim (province,municipality) VALUES (%s, %s)", (row[0], row[1]) )

    

print("=====================================")
print('Done with location dim')


print("=====================================")
print("insert into thing dim")


# Read the last row from the thing_dim table
cur.execute("SELECT thing_id FROM thing_dim ORDER BY thing_id DESC LIMIT 1")
#check if the cursor result is not empty


    

result = cur.fetchone()
#check if the last row is empty
if result is not None:
    last_thing_id = result[0]
    # last_thing_id = 

    print('Last thing_id:', last_thing_id)
else:
    last_thing_id = 0
    print('No data found in df_last_row of thing')

# Create a cursor object for MySQL
cursor_mysql = cnx.cursor()

cursor_mysql.execute(f"""
SELECT
	t.thing_id,
	tt.thing_type_designation,
	t.thing_designation,
	t.thing_matricule AS thing_plate,
	t.thing_serial_number ,
	t.entreprise_id AS company_id,
	t.thing_group_id AS group_id,
	t.thing_type_id AS type_id,
	tg.thing_group_designation,
	thing_marque_id
FROM
	geopfe.thing t,
	geopfe.thing_type tt,
	thing_group tg
WHERE
	t.thing_type_id = tt.thing_type_id
	AND tg.thing_group_id = t.thing_group_id

                        AND t.thing_id > {last_thing_id}
                                        """)


rows = cursor_mysql.fetchall()




# check if rows is not empty
if rows:
    print('Rows:')

    # insert this row into the thing_dim table
    insert_query = """
    INSERT INTO thing_dim (thing_id, thing_type_designation, thing_designation, thing_plate,thing_serial ,company_id, group_id ,type_id, group_name, thing_mark)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_query, rows)

    
print("=====================================")
print('done with thing dim')




print("=====================================")
print('insert into car monitoring')






select_query = "SELECT * FROM date_dim"
cur.execute(select_query)
rows = cur.fetchall()




# Drop the MySQL table if it exists
drop_table_query = "DROP TABLE IF EXISTS date_dim"
cursor_mysql.execute(drop_table_query)
# Create the MySQL table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS date_dim (
        date_id INT PRIMARY KEY,
            full_date DATE,
            year INT,
            month INT,
            day INT,
            month_year VARCHAR(7),
            month_name VARCHAR(20),
            quarter VARCHAR(7),
            day_type VARCHAR(10),
            season VARCHAR(10)
)
"""
cursor_mysql.execute(create_table_query)



formatted_rows = [(row[0], row[1].strftime('%Y-%m-%d'), row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]) for row in rows]



# Insert the rows into the MySQL table
for row in formatted_rows:
    insert_query = """
    INSERT INTO date_dim (date_id, full_date, year, month, day, month_year, month_name, quarter, day_type, season)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor_mysql.execute(insert_query, row)


# Commit the changes to the MySQL database
cnx.commit()


print("=====================================")
print('date_dim copied')

print("=====================================")
print('last date_id:', last_date_id)




# Execute the query to join the tables from MySQL and PostgreSQL
query = f"""
SELECT 
    SUM(CASE WHEN engine_status = 0 THEN TIMESTAMPDIFF(SECOND, prev_trace_date, trace_date) ELSE 0 END) / 3600 AS idle_time,
    SUM(CASE WHEN engine_status = 1 THEN TIMESTAMPDIFF(SECOND, prev_trace_date, trace_date) ELSE 0 END) / 3600 AS active_time,
    SUM(
        111.045 * DEGREES(ACOS(COS(RADIANS(prev_latitude))
             * COS(RADIANS(latitude))
             * COS(RADIANS(prev_longitude - longitude))
             + SIN(RADIANS(prev_latitude))
             * SIN(RADIANS(latitude)))))
    AS total_distance_traveled,

    AVG(speed) AS avg_speed,
    MAX(speed) AS max_speed,
        date_id,
    thing_id
FROM (
    SELECT 
        DISTINCT
        d.date_id,
        thing_id,
        latitude,
        longitude,
        speed,
        engine_status,
        trace_date,
        LAG(latitude) OVER (PARTITION BY thing_id ORDER BY trace_date) AS prev_latitude,
        LAG(longitude) OVER (PARTITION BY thing_id ORDER BY trace_date) AS prev_longitude,
        LAG(trace_date) OVER (PARTITION BY thing_id ORDER BY trace_date) AS prev_trace_date
    FROM 
        trace_week t
    JOIN 
        date_dim d ON t.trace_date_day = d.full_date
        WHERE      thing_id <= 4333 AND d.date_id > {last_date_id}
) AS subquery
WHERE 
    prev_latitude IS NOT NULL
    AND prev_longitude IS NOT NULL
GROUP BY 
    thing_id, date_id;



"""

print('inserting into car monitoring')

cursor_mysql.execute(query)

# Fetch all the rows from the result set
rows = cursor_mysql.fetchall()



# Print the first 5 rows
# for row in rows[:5]:
#     print(row)

# Insert the rows into the PostgreSQL table
for row in rows:
    insert_query = """
    INSERT INTO vehicle_peroformance (idle_time, active_time, travled_distance, avg_speed, max_speed, date_id, thing_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(insert_query, row)

# Commit the changes to the PostgreSQL database
conn.commit()

print('done with fact table')







# load data into the data warehouse


cnx.commit()


conn.commit()
cur.close()
conn.close()
print("=====================================")
print("=====================================")
print("=====================================")
print("ALL data is loaded successfully")
print("=====================================")
print("=====================================")
print("==============END====================")