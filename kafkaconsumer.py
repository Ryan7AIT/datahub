from math import atan2, cos, radians, sin, sqrt
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType,IntegerType
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from pyspark.sql import Window


if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaConsumer")\
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
        .config("spark.sql.cassandra.read.partitioning.strategy", "KeyGroupedPartitioner") \
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




    existing_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
            .options(table="trace", keyspace="pfe", partitioner="org.apache.spark.sql.cassandra.KeyGroupedPartitioner") \
        .load()
    

    # read data from existing_df and when data is noisy replace with the avg of the last 5 values
# Define window specification to get the last element
    window_spec = Window.partitionBy("thing_id").orderBy(F.col("trace_date").desc())

    # Get the last element for each thing_id in existing_df
    last_element_df = existing_df.withColumn("last_speed", F.last("speed").over(window_spec)) \
                                .select("thing_id", "last_speed").distinct()

    # Join the df with last_element_df to get the last speed value
    df = df.join(last_element_df, on="thing_id", how="left")

    # Replace speed with the last speed value if speed is greater than 100
    df = df.withColumn(
        "speed",
        F.when(F.col("speed") > 200, F.col("last_speed")).otherwise(F.col("speed"))
    ).drop("last_speed")



    # # Start the query to print the output to the console
    # query = df \
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