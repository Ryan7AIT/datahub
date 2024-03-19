from kafka import KafkaConsumer
from cassandra.cluster import Cluster

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic = 't'
consumer_group_id = 'your_consumer_group_i2d'

# Cassandra configuration
cassandra_host = 'localhost'
cassandra_keyspace = 'iotdata'
cassandra_table = 'test_table'

# Connect to Kafka
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         group_id=consumer_group_id)

# Connect to Cassandra
cluster = Cluster([cassandra_host])
session = cluster.connect(cassandra_keyspace)

# Create the table if it doesn't exist
create_table_query = f"CREATE TABLE IF NOT EXISTS {cassandra_table} (id UUID PRIMARY KEY, value TEXT);"
session.execute(create_table_query)

# Consume Kafka events and insert into Cassandra
for message in consumer:
    event = message.value.decode('utf-8')
    insert_query = f"INSERT INTO {cassandra_table} (id, value) VALUES (uuid(), '{event}');"
    session.execute(insert_query)