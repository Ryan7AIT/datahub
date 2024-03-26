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
                date_id int,
                full_date text,
                year int,
                month int,
                day int,
                month_year text,
                month_name text,
                quarter text,
                day_type text,
                season text,
                thing_id text,
                thing_name text,
                thing_type text,
                thing_plate text,
                PRIMARY KEY (thing_id)
) ;
                """
                )



# Close the connection
session.shutdown()
cluster.shutdown()

