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
''')




# Create the second dimension table
cur.execute('''
    CREATE TABLE thing (
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
            commune_id INT,
            commune_designation varchar(50),
            wilaya_id INT,
            wilaya_designation varchar(50),
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


#


# Create the fact table
cur.execute('''
    CREATE TABLE vehicle_peroformance (
                        idle_time FLOAT,
                        active_time FLOAT,

            travled_distance FLOAT,
            avg_speed FLOAT,
            max_speed FLOAT,
      
            date_id INT REFERENCES date_dim(date_id),
            thing_id INT REFERENCES thing_dim(thing_id),
             PRIMARY KEY (date_id, thing_id)           
                )
''')



# Commit the changes to the database
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()