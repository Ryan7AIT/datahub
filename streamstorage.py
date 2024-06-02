from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['localhost'])  # Replace 'localhost' with your Cassandra cluster address
session = cluster.connect()

# Use the session to execute queries

# Perform your operations here

# creaye a keyspace
session.execute("""
                CREATE KEYSPACE IF NOT EXISTS pfe
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
                """
               )

session.execute("USE pfe")  # Replace 'keyspace_name' with your keyspace name

# Create a sample table
session.execute("""
                
                CREATE TABLE IF NOT EXISTS pfe.trace (
    trace_id text,
    thing_id text,
    trace_date text,
    long double,
    lat double,
    speed int,
    engine_status text,
    oil_value double,
    fuel_liters int,
    fuel_percent double,
                km_after_last_maintance double,
                battery double,
    PRIMARY KEY (thing_id, trace_date)
) WITH CLUSTERING ORDER BY (trace_date DESC);
                """
               )


# create analytic object table
session.execute("""
                
                CREATE TABLE IF NOT EXISTS pfe.vehicle_performance (
                idle_time int,
                active_time int,
                traveled_distance int,
                avg_speed int,
                max_speed int,
                full_date text,
                year int,
                month int,
                day int,
                month_year text,
                month_name text,
                quarter text,
                day_type text,
                season text,
                thing_id int,
                type_id int,
                group_id int,
                latitude double,
                longitude double,
                trace_date text,
                maintenance int,
                rul float,
                fuel int,
                PRIMARY KEY (thing_id)
) ;
                """
                )


# create analytic object table alert
session.execute("""
                
                CREATE TABLE IF NOT EXISTS pfe.alert (
                alert_id int,
                type_id int,
                event_id int,
                user_id int,
                province text,
                municipality text,
                alert_number int,
                full_date text,
                year int,
                month int,
                day int,
                month_year text,
                month_name text,
                quarter text,
                day_type text,
                season text,
                PRIMARY KEY (type_id,event_id, user_id)
);
                """
                )



# Close the connection
session.shutdown()
cluster.shutdown()

