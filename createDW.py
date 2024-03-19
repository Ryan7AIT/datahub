import psycopg2

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe",
    user="postgres",
    password="ryqn"
)

print('111')
# Create a cursor object to interact with the database
cur = conn.cursor()

# Perform database operations here



# Drop the fact table if it exists
cur.execute('DROP TABLE IF EXISTS vehicle_peroformance')

# Drop the first dimension table if it exists
cur.execute('DROP TABLE IF EXISTS date_dim')

# Drop the second dimension table if it exists
cur.execute('DROP TABLE IF EXISTS vehicle_dim')

# Drop the third dimension table if it exists
cur.execute('DROP TABLE IF EXISTS location_dim')

cur.execute('DROP TABLE IF EXISTS thing_dim')




# Create the first dimension table
cur.execute('''
    CREATE TABLE date_dim (
        date_id SERIAL PRIMARY KEY,
            hour INT,
            day INT,
            month INT,
            monthname VARCHAR(20),
            year INT,
            quarter INT
    )
''')

# Create the second dimension table
cur.execute('''
    CREATE TABLE vehicle_dim (
         vehicle_id SERIAL PRIMARY KEY,
            vehicle_name VARCHAR(50),
            vehicle_type VARCHAR(50),
            vehicle_model VARCHAR(50),
            vehicle_color VARCHAR(50),
            vehicle_year INT,
            vehicle_brand VARCHAR(50) ,
            car_plate VARCHAR(50),
            vehicle_owner VARCHAR(50),
            carburant_type VARCHAR(50)
    )
''')

# Create the third dimension table
cur.execute('''
    CREATE TABLE location_dim (
        location_id SERIAL PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT,
            altitude FLOAT,
            country VARCHAR(50),
            city VARCHAR(50),
            street VARCHAR(50),
            postal_code VARCHAR(50),
            region VARCHAR(50)
    )
''')


cur.execute('''
            CREATE TABLE thing_dim (
             thing_id SERIAL PRIMARY KEY,
            thing_type VARCHAR(50),
            thing_name TEXT,
            thing_plate VARCHAR(50)
            
                )
''')



# Create the fact table
cur.execute('''
    CREATE TABLE vehicle_peroformance (
        fact_id SERIAL PRIMARY KEY,
            travled_distance FLOAT,
            avg_speed FLOAT,
            max_speed FLOAT,
            idle_time FLOAT,
            active_time FLOAT,
            battery_level FLOAT,
            first_start_time TIMESTAMP,
            last_stop_time TIMESTAMP,
            date_id INT REFERENCES date_dim(date_id),
            thing_id INT REFERENCES thing_dim(thing_id),
            location_id INT REFERENCES location_dim(location_id)
            
                )
''')



# Commit the changes to the database
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()