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




# Close the connection
session.shutdown()
cluster.shutdown()

