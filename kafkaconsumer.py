from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType,IntegerType

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
        .add("fuel_percent", IntegerType())

    # Deserialize the JSON data
    df = df.select(from_json(col("value").cast("string"), schema).alias("data"))


    # make the df in atbale format
    df = df.selectExpr("data.trace_id", "data.thing_id", "data.trace_date", "data.longitude", "data.latitude", "data.speed", "data.engine_status", "data.oil_value", "data.fuel_liters", "data.fuel_percent")

    #rename a cloumn
    df = df.withColumnRenamed("latitude","lat")
    df = df.withColumnRenamed("longitude","long")
    

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
    # query = updated_df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()

    # query.awaitTermination()


    # Define function to write DataFrame to Cassandra
    def write_to_cassandra(df, epoch_id):
        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="trace", keyspace="pfe") \
          .mode("append") \
          .save()

    # Write the streaming data to Cassandra
    query = df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_cassandra) \
        .trigger(processingTime='2 seconds') \
        .start()

    query.awaitTermination()