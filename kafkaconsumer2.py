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

if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaConsumer")\
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
        .getOrCreate()
    
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


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
        .add("long", DoubleType()) \
        .add("lat", DoubleType()) \
        .add("speed", IntegerType()) \
        .add("latitute", DoubleType()) \
        .add("longitude", DoubleType()) \
        .add("engine_status", StringType())

    # Deserialize the JSON data
    df = df.select(from_json(col("value").cast("string"), schema).alias("data"))


    # make the df in atbale format
    df = df.selectExpr("data.trace_id", "data.thing_id", "data.trace_date", "data.long", "data.lat", "data.speed", "data.engine_status","data.latitute","data.longitude")
    

    

    # # Read the existing data from Cassandra
    existing_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="vehicle_performance", keyspace="pfe") \
        .load()
    

    updated_df = df.alias('new').join(existing_df.alias('old'), 'thing_id', 'leftouter')

    # Calculate the new measurements based on the event data and the current state
    # TODO: Replace this with your actual calculation logic
    # updated_df = updated_df.withColumn("avg_speed", col("old.avg_speed") + col("new.speed"))
    #cehck if column is not null
    updated_df = updated_df.withColumn('avg_speed', F.when(F.col('old.avg_speed').isNull(), F.col('new.speed')).otherwise((F.col('new.speed') + F.col('old.avg_speed')) /2))
    updated_df = updated_df.withColumn('max_speed', F.when(F.col('old.max_speed').isNull(), F.col('new.speed')).otherwise(F.when(F.col('new.speed') > F.col('old.max_speed'), F.col('new.speed')).otherwise(F.col('old.max_speed'))))
    updated_df = updated_df.withColumn('idle_time', F.when(F.col('old.idle_time').isNull(), 0).otherwise(F.when(F.col('new.engine_status') == 'stoped', F.col('old.idle_time') + ((F.unix_timestamp('new.trace_date') - F.unix_timestamp('old.trace_date')) )).otherwise(F.col('old.active_time'))))
    updated_df = updated_df.withColumn('active_time', F.when(F.col('old.active_time').isNull(), 0).otherwise(F.when(F.col('new.engine_status') == 'running', F.col('old.active_time') + ((F.unix_timestamp('new.trace_date') - F.unix_timestamp('old.trace_date')) )).otherwise(F.col('old.active_time'))))



    # updated_df = updated_df.withColumn('idle_time', F.when(F.col('old.idle_time').isNull(), 0).otherwise(F.when(F.col('new.engine_status') == 'stoped', F.col('old.idle_time') + 1).otherwise(F.col('old.active_time'))))
    
    # updated_df = updated_df.withColumn('active_time', F.when(F.col('old.active_time').isNull(), 1).otherwise(F.when(F.col('new.engine_status') == 'running', F.col('old.active_time') + 1).otherwise(F.col('old.active_time'))))

    # Define the haversine function
    def haversine(lat1, lon1, lat2, lon2):
        # R = 6371.0
        # lat1 = radians(lat1)
        # lon1 = radians(lon1)
        # lat2 = radians(lat2)
        # lon2 = radians(lon2)
        # dlon = lon2 - lon1
        # dlat = lat2 - lat1
        # a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        # c = 2 * atan2(sqrt(a), sqrt(1 - a))
        # distance = R * c
        return 1

    # updated_df = updated_df.withColumn('active_time', F.when(F.col('new.engine_status') == 'running', F.col('old.active_time') + 1).otherwise(F.col('old.active_time')))
    # updated_df = updated_df.withColumn('total_distance', F.when(F.col('old.total_distance').isNull(), 0).otherwise(F.col('old.total_distance') + F.sqrt((F.col('new.long') - F.col('old.long'))**2 + (F.col('new.lat') - F.col('old.lat'))**2)))
    # updated_df = updated_df.withColumn('total_distance', F.when(F.col('old.total_distance').isNull(), 0).otherwise(F.col('old.total_distance') + 1))


    # Register the haversine function as a UDF
    haversine_udf = F.udf(haversine, FloatType())

    # Calculate the distance and add it to the total distance
    # updated_df = updated_df.withColumn(
    #     'traveled_distance', 
    #     F.when(
    #         F.col('old.traveled_distance').isNull(), 
    #         0
    #     ).otherwise(
    #         F.col('old.traveled_distance') + haversine_udf(F.col('old.latitute'), F.col('old.longitude'), F.col('new.latitute'), F.col('new.longitude'))
    #     )
    # )


    updated_df = updated_df.withColumn(
    'traveled_distance', 
    F.when(
        F.col('old.traveled_distance').isNull() | F.col('old.latitute').isNull() | F.col('old.longitude').isNull() | F.col('new.latitute').isNull() | F.col('new.longitude').isNull(), 
        0
    ).otherwise(
        F.col('old.traveled_distance') + haversine_udf(F.col('old.latitute'), F.col('old.longitude'), F.col('new.latitute'), F.col('new.longitude'))
    )
)

    # add date_id column
    updated_df = updated_df.withColumn("date_id", col("new.trace_date").substr(1, 10).cast(IntegerType()))

    # select the columns to be saved in cassandra
    current_date = datetime.now()

    formatted_date = current_date.strftime('%A %Y-%m-%d')


    updated_df = updated_df.withColumn("full_date", F.lit(formatted_date))

    updated_df = updated_df.select("new.thing_id", "date_id",  "avg_speed", "max_speed", "idle_time", "active_time", "full_date","new.longitude","new.latitute","new.trace_date","traveled_distance")

        # Load the model from the file
    with open('speeding_model.pkl', 'rb') as file:
        model = pickle.load(file)

    # Define a user-defined function that applies the model
    @udf(IntegerType())
    def predict_speeding(speed):
        prediction = model.predict(np.array([[speed]]))
        return int(prediction[0])

    # Apply the model to the incoming DataFrame
    updated_df = updated_df.withColumn('is_speeding', predict_speeding(updated_df['max_speed']))


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
          .options(table="vehicle_performance", keyspace="pfe") \
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

