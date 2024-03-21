import psycopg2


import mysql.connector
try:
    # Establish a connection to the MySQL server
    cnx = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="geopfe"
    )

    # Establish a connection to the PostgreSQL server
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="geopfe",
        user="postgres",
        password="ryqn"
    )

    # Create a cursor object for MySQL
    cursor_mysql = cnx.cursor()

    # Create a cursor object for PostgreSQL
    cursor_postgres = conn.cursor()


    # Select a table from MySQL and print it
    select_query = "SELECT * FROM date_dim"
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()




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

except mysql.connector.Error as err:
    print("MySQL Error:", err)

# Commit the changes to the MySQL database
cnx.commit()

# INSERT INTO vehicle_peroformance (travled_distance,  avg_speed, max_speed,idle_time, active_time,date_id,thing_id)




# Execute the query to join the tables from MySQL and PostgreSQL
query = """
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
        WHERE      thing_id <= 4333
        and speed <= 300
) AS subquery
WHERE 
    prev_latitude IS NOT NULL
    AND prev_longitude IS NOT NULL
GROUP BY 
    thing_id, date_id;



"""
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
    cursor_postgres.execute(insert_query, row)

# Commit the changes to the PostgreSQL database
conn.commit()

# Close the cursors
cursor_mysql.close()
cursor_postgres.close()



# Close the connections
cnx.close()
conn.close()

