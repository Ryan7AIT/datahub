from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType,IntegerType
from pyspark.sql import functions as F

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
        .add("long", DoubleType()) \
        .add("lat", DoubleType()) \
        .add("speed", IntegerType()) \
        .add("engine_status", StringType())

    # Deserialize the JSON data
    df = df.select(from_json(col("value").cast("string"), schema).alias("data"))


    # make the df in atbale format
    df = df.selectExpr("data.trace_id", "data.thing_id", "data.trace_date", "data.long", "data.lat", "data.speed", "data.engine_status")
    

    

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
    updated_df = updated_df.withColumn('idle_time', F.when(F.col('old.idle_time').isNull(), 1).otherwise(F.when(F.col('new.engine_status') == 'stoped', F.col('old.idle_time') + 1).otherwise(F.col('old.active_time'))))
    updated_df = updated_df.withColumn('active_time', F.when(F.col('old.active_time').isNull(), 1).otherwise(F.when(F.col('new.engine_status') == 'running', F.col('old.active_time') + 1).otherwise(F.col('old.active_time'))))


    # updated_df = updated_df.withColumn('active_time', F.when(F.col('new.engine_status') == 'running', F.col('old.active_time') + 1).otherwise(F.col('old.active_time')))
    # updated_df = updated_df.withColumn('total_distance', F.when(F.col('old.total_distance').isNull(), 0).otherwise(F.col('old.total_distance') + F.sqrt((F.col('new.long') - F.col('old.long'))**2 + (F.col('new.lat') - F.col('old.lat'))**2)))
    # updated_df = updated_df.withColumn('total_distance', F.when(F.col('old.total_distance').isNull(), 0).otherwise(F.col('old.total_distance') + 1))

    # add date_id column
    updated_df = updated_df.withColumn("date_id", col("new.trace_date").substr(1, 10).cast(IntegerType()))

    # select the columns to be saved in cassandra
    updated_df = updated_df.select("new.thing_id", "date_id",  "avg_speed", "max_speed", "idle_time", "active_time")

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