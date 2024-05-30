import psycopg2

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe",
    user="postgres",
    password="ryqn"
)

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

cur.execute('drop table if exists alert_fact')


cur.execute('drop table if exists alert_deg_dim')

cur.execute('drop table if exists event_dim')

cur.execute('drop table if exists user_dim')


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






# Create the third dimension table
cur.execute('''
    CREATE TABLE location_dim (
        location_id SERIAL PRIMARY KEY,
            municipality varchar(50),
            province varchar(50)
         )
''')


cur.execute('''
            CREATE TABLE thing_dim (
            thing_id SERIAL PRIMARY KEY,
            thing_type_designation VARCHAR(150),
            thing_designation VARCHAR(150),
            thing_plate VARCHAR(150),
            thing_serial VARCHAR(150),
            company_id bigint,
            company_name VARCHAR(150),
            group_id bigint,
            type_id INT,
            thing_mark VARCHAR(150),
            group_name VARCHAR(150)
            
                )
''')

print('============================================================')
print('thing_dim table created')        
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

print('============================================================')
print('vehicle_peroformance table created')



cur.execute('''
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
cur.execute("""
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


cur.execute('''
        CREATE TABLE IF NOT EXISTS alert_deg_dim (
            alert_degree_id SERIAL PRIMARY KEY,
            alert_degree VARCHAR(50)
        )
    ''')

print('============================================================')

print('alert_deg_dim table created')


cur.execute('''
        CREATE TABLE IF NOT EXISTS event_dim (
            event_id SERIAL PRIMARY KEY,
            event_designation VARCHAR(50)
        )
    ''')

print('============================================================')
print('event_dim table created')

cur.execute('''
            CREATE TABLE IF NOT EXISTS user_dim (
            user_id SERIAL PRIMARY KEY,
            user_name VARCHAR(150),
            company_id bigint,
            company_name VARCHAR(150)
            )
            ''')

print('============================================================')
print('user_dim table created')


# create alert fact table
cur.execute('''
    CREATE TABLE IF NOT EXISTS alert_fact (
        alert_id SERIAL PRIMARY KEY,
        location_id INT,
        date_id INT,
        type_id INT,
        user_id INT,
        event_id INT,
        alert_count INT
        

    )
''')
print('============================================================')
print('alert_fact table created')


# Print a message to the console



print('============================================================')
print('ALL TABLES CREATED')
print('============================================================')


# Commit the changes to the database
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()