from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['localhost'])  # Replace 'localhost' with your Cassandra cluster address
session = cluster.connect()

# Use the session to execute queries
session.execute("USE keyspace_name")  # Replace 'keyspace_name' with your keyspace name

# Perform your operations here

# Create a sample table
session.execute("CREATE TABLE IF NOT EXISTS my_table (id UUID PRIMARY KEY, name TEXT)")

# Close the connection
session.shutdown()
cluster.shutdown()

