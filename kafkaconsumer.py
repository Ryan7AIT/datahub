from math import atan2, cos, radians, sin, sqrt
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType,IntegerType
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaConsumer")\
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "iotevents") \
        .option("startingOffsets", "latest") \
        .load()

    # Define your schema (replace with your actual schema)
    schema = StructType() \
        .add("trace_id", StringType()) \
        .add("thing_id", IntegerType()) \
        .add("trace_date", StringType()) \
        .add("longitude", DoubleType()) \
        .add("latitude", DoubleType()) \
        .add("speed", IntegerType()) \
        .add("engine_status", StringType())\
        .add("oil_value", IntegerType())\
        .add("fuel_liters", IntegerType())\
        .add("fuel_percent", IntegerType())\
        .add("battery",DoubleType())
    

    # Deserialize the JSON data
    df = df.select(from_json(col("value").cast("string"), schema).alias("data"))


    # make the df in atbale format
    df = df.selectExpr("data.trace_id", "data.thing_id","data.battery", "data.trace_date", "data.longitude", "data.latitude", "data.speed", "data.engine_status", "data.oil_value", "data.fuel_liters", "data.fuel_percent")

    #rename a cloumn
    df = df.withColumnRenamed("latitude","lat")
    df = df.withColumnRenamed("longitude","long")




#     # Define the haversine function
#     def haversine(lat1, lon1, lat2, lon2):
#         R = 6371.0
#         lat1 = radians(lat1)
#         lon1 = radians(lon1)
#         lat2 = radians(lat2)
#         lon2 = radians(lon2)
#         dlon = lon2 - lon1
#         dlat = lat2 - lat1
#         a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
#         c = 2 * atan2(sqrt(a), sqrt(1 - a))
#         distance = R * c
#         return distance


#     # Register the haversine function as a UDF
#     haversine_udf = F.udf(haversine, FloatType())



#     df = df.withColumn(
#     'km_after_last_maintenance', 
#     F.when(
#         F.col('old.km_after_last_maintenance').isNull() | F.col('old.latitude').isNull() | F.col('old.longitude').isNull() | F.col('new.latitude').isNull() | F.col('new.longitude').isNull(), 
#         0
#     ).otherwise(
#         F.col('old.km_after_last_maintenance') + haversine_udf(F.col('old.latitude'), F.col('old.longitude'), F.col('new.latitude'), F.col('new.longitude'))
#     )
# )
    
    

    # # Read the existing data from Cassandra
    # existing_df = spark.read \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .options(table="vehicle_performance", keyspace="pfe") \
    #     .load()
    

    # updated_df = df.alias('new').join(existing_df.alias('old'), 'thing_id', 'leftouter')

    # Calculate the new measurements based on the event data and the current state
    # TODO: Replace this with your actual calculation logic
    # updated_df = updated_df.withColumn("avg_speed", col("old.avg_speed") + col("new.speed"))

    # Start the query to print the output to the console
    query = df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()


    # Define function to write DataFrame to Cassandra
    def write_to_cassandra(df, epoch_id):
        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="trace", keyspace="pfe") \
          .mode("append") \
          .save()

    # # Write the streaming data to Cassandra
    # query = df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(write_to_cassandra) \
    #     .trigger(processingTime='2 seconds') \
    #     .start()

    # query.awaitTermination()