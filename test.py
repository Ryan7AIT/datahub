from geopy.geocoders import Nominatim

def get_street_address(latitude, longitude):
    geolocator = Nominatim(user_agent="my_app")
    location = geolocator.reverse((latitude, longitude))
    address = location.raw['address']

    street = address.get('road', '')
    city = address.get('city', '')
    state = address.get('state', '')
    return address
# Example usage
# latitude = 36.685685
# longitude = 3.354008
# street = get_street_address(latitude, longitude)
# # print(f"Street: {street}")
# # print(f"City: {city}")
# # print(f"State: {state}")

# print(street)

from pyspark.sql import functions as F

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Python Spark SQL") \
    .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
    .getOrCreate()

# Load the Cassandra table into a DataFrame
cassandra_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="vehicle_performance", keyspace="pfe") \
    .load()

# Load the PostgreSQL table into a DataFrame
# postgres_df = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/your_database") \
#     .option("dbtable", "postgres_table") \
#     .option("user", "your_username") \
#     .option("password", "your_password") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

# Query for total_distance for each date
# cassandra_result = cassandra_df.sum("avg_speed").collect()
cassandra_result = cassandra_df.agg(F.sum("avg_speed")).collect()

print(cassandra_result)
# postgres_result = postgres_df.groupBy("date").sum("total_distance").collect()

# Combine results
# combined_result = cassandra_result + postgres_result

# Create two lists: one for the distances and one for the dates
# distances = [row[1] for row in combined_result]
# dates = [row[0] for row in combined_result]

# Now distances contains the total_distance for each date and dates contains the corresponding dates
