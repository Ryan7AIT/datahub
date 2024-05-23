from array import ArrayType
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
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

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
    

    # updated_df = df.alias('new').join(existing_df.alias('old'), 'user_id', 'leftouter')

    updated_df = df.alias('new').join(
    existing_df.alias('old'), 
    (F.col('new.user_id') == F.col('old.user_id')) & (F.col('new.type_id') == F.col('old.type_id')), 
    'leftouter'
)



    # calculate the number of alerts
    updated_df = updated_df.withColumn('alert_number', F.when(updated_df['old.alert_number'].isNull(), 1).otherwise(updated_df['old.alert_number'] + 1))

    
    # get the location of the alert
    def get_street_address(p):
        latitude, longitude = p.split(',')
        geolocator = Nominatim(user_agent="my_app", timeout=10)  

        try:
            location = geolocator.reverse((latitude, longitude))
            address = location.raw['address']
            return address['state']
        except GeocoderTimedOut:
            return get_street_address(p)
        except Exception as e:
            return "default"
        

    def get_street_address2(p):
        latitude, longitude = p.split(',')
        geolocator = Nominatim(user_agent="my_app", timeout=10)  

        try:
            location = geolocator.reverse((latitude, longitude))
            address = location.raw['address']
            return  address['town']
        except GeocoderTimedOut:
            return get_street_address2(p)
        except Exception as e:
            return "default"
        

    # Register the UDF
    get_address = udf(get_street_address)
    get_address2 = udf(get_street_address2)


    # Create updated_df with the new 'address' column
    updated_df = updated_df.withColumn('municipality', get_address2(F.concat(col('latitude'), lit(','), col('longitude'))))
    updated_df = updated_df.withColumn('province', get_address(F.concat(col('latitude'), lit(','), col('longitude'))))

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


    updated_df = updated_df.select('alert_number','day','day_type','new.event_id','full_date','month','month_name','month_year','municipality','province','quarter','season','new.type_id','new.user_id','year')
   



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
          .options(table="alert", keyspace="pfe") \
          .mode("append") \
          .save()

    
    # Write the streaming data to Cassandra
    query = updated_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_cassandra) \
        .trigger(processingTime='2 seconds') \
        .start()

    query.awaitTermination()

