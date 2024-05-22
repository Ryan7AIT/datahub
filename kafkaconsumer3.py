from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType,IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2
from pyspark.sql.types import FloatType
import pickle
import numpy as np
from pyspark.sql.functions import lit
from keras.models import load_model
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaConsumer")\
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
        .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC") \
        .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "iotalerts") \
        .option("startingOffsets", "latest") \
        .load()
    

 

    # Define your schema (replace with your actual schema)
    schema = StructType() \
        .add("type_id", StringType()) \
        .add("user_id", IntegerType()) \
        .add("event_date", StringType()) \
        .add("location", IntegerType()) \
        .add("latitude", DoubleType()) \
        .add("longitude", DoubleType()) \
        .add("event_id", StringType())\
        .add("type_id", IntegerType())\
  

    # Deserialize the JSON data
    df = df.select(from_json(col("value").cast("string"), schema).alias("data"))


    # make the df in atbale format
    df = df.selectExpr("data.*")
    

    

    # # Read the existing data from Cassandra
    existing_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="alert", keyspace="pfe") \
        .load()
    

    updated_df = df.alias('new').join(existing_df.alias('old'), 'user_id', 'leftouter')

    

 

    # select the columns to be saved in cassandra
    current_date = datetime.now()

    formatted_date = current_date.strftime('%A %Y-%m-%d')


    # Get today's date
    today = F.current_date()

    # Add the new attributes to the DataFrame
    updated_df = updated_df.withColumn('full_date', F.date_format(today, 'yyyy-MM-dd'))
    updated_df = updated_df.withColumn('year', F.year(today))
    updated_df = updated_df.withColumn('month', F.month(today))
    updated_df = updated_df.withColumn('day', F.dayofmonth(today))
    updated_df = updated_df.withColumn('month_year', F.date_format(today, 'yyyy-MM'))
    updated_df = updated_df.withColumn('month_name', F.date_format(today, 'MMMM'))
    updated_df = updated_df.withColumn('quarter', F.quarter(today))
    updated_df = updated_df.withColumn('day_type', F.when(F.dayofweek(today).isin([1, 7]), 'Weekend').otherwise('Weekday'))
    updated_df = updated_df.withColumn('season', F.when(F.month(today).isin([12, 1, 2]), 'Winter')
                                    .when(F.month(today).isin([3, 4, 5]), 'Spring')
                                    .when(F.month(today).isin([6, 7, 8]), 'Summer')
                                    .otherwise('Fall'))


   



    # Start the query to print the output to the console
    query = updated_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()


    # Define function to write DataFrame to Cassandra
    def write_to_cassandra(df, epoch_id):
        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="vehicle_performance", keyspace="pfe") \
          .mode("append") \
          .save()

    
    # # Write the streaming data to Cassandra
    # query = updated_df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(write_to_cassandra) \
    #     .trigger(processingTime='2 seconds') \
    #     .start()

    # query.awaitTermination()

