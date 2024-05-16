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
        .option("subscribe", "iotevents") \
        .option("startingOffsets", "latest") \
        .load()

    # Define your schema (replace with your actual schema)
    schema = StructType() \
        .add("trace_id", StringType()) \
        .add("thing_id", IntegerType()) \
        .add("trace_date", StringType()) \
        .add("speed", IntegerType()) \
        .add("latitude", DoubleType()) \
        .add("longitude", DoubleType()) \
        .add("engine_status", StringType())\
        .add("oil_value", IntegerType())\
        .add("fuel_liters", IntegerType())\
        .add("fuel_percent", IntegerType())

    # Deserialize the JSON data
    df = df.select(from_json(col("value").cast("string"), schema).alias("data"))


    # make the df in atbale format
    df = df.selectExpr("data.trace_id", "data.thing_id", "data.trace_date", "data.speed", "data.engine_status","data.latitude","data.longitude","data.oil_value","data.fuel_liters","data.fuel_percent")
    

    

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
    #         F.col('old.traveled_distance') + haversine_udf(F.col('old.latitude'), F.col('old.longitude'), F.col('new.latitude'), F.col('new.longitude'))
    #     )
    # )


    updated_df = updated_df.withColumn(
    'traveled_distance', 
    F.when(
        F.col('old.traveled_distance').isNull() | F.col('old.latitude').isNull() | F.col('old.longitude').isNull() | F.col('new.latitude').isNull() | F.col('new.longitude').isNull(), 
        0
    ).otherwise(
        F.col('old.traveled_distance') + haversine_udf(F.col('old.latitude'), F.col('old.longitude'), F.col('new.latitude'), F.col('new.longitude'))
    )
)

    # add date_id column
    # updated_df = updated_df.withColumn("date_id", col("new.trace_date").substr(1, 10).cast(IntegerType()))

    # select the columns to be saved in cassandra
    current_date = datetime.now()

    formatted_date = current_date.strftime('%A %Y-%m-%d')


    updated_df = updated_df.withColumn("full_date", F.lit(formatted_date))


    to_predict = updated_df.select("new.engine_status","new.oil_value","new.fuel_liters","new.thing_id")
    # to_predict = updated_df.select("new.engine_status","new.oil_value")


    to_predict = to_predict.withColumn("last_oil_change", lit(1))
    to_predict = to_predict.withColumn("car_age", lit(1))
    to_predict = to_predict.withColumn("fuel_change", lit(2))
    to_predict = to_predict.withColumn("power_supply_voltage", lit(10))


    updated_df = updated_df.withColumn("last_oil_change", lit(1))
    updated_df = updated_df.withColumn("car_age", lit(1))
    updated_df = updated_df.withColumn("fuel_change", lit(2))
    updated_df = updated_df.withColumn("power_supply_voltage", lit(10))





    updated_df = updated_df.select("last_oil_change", "car_age","fuel_change","power_supply_voltage","new.thing_id",  "avg_speed", "max_speed", "idle_time", "active_time", "full_date","new.longitude","new.latitude","new.trace_date","traveled_distance","oil_value","engine_status","fuel_liters","fuel_percent")



    #     # Load the model from the file
    # with open('speeding_model.pkl', 'rb') as file:
    #     model = pickle.load(file)

        # Later, load the model\

  

    # Define a user-defined function that applies the model
    # @udf(IntegerType())
    # def predict_speeding(engine_status, power_supply_voltage, oil_value, fuel_liters, fuel_change, car_age, last_oil_change):
    #     # prediction = model.predict(np.array([[engine_status, power_supply_voltage, oil_value, fuel_liters, fuel_change, car_age, last_oil_change]]))
    #     # prediction = model.predict(np.array([[speed]]))
    #     return 1
    #     # return int(prediction[0])

    # @udf(IntegerType())
    # def predict_speeding2(engine_status):
    #     # prediction = model.predict(np.array([[engine_status, power_supply_voltage, oil_value, fuel_liters, fuel_change, car_age, last_oil_change]]))
    #     # prediction = model.predict(np.array([[speed]]))
    #     return 1
    

    # Define a user-defined function that applies the model
    # @udf(IntegerType())
    # def predict_speeding(row):
    #     features = [row['engine_status'], row['power_supply_voltage'], row['oil_value'], row['fuel_liters'], row['fuel_change'], row['car_age'], row['last_oil_change']]
    #     prediction = model.predict([features])
    #     return int(prediction[0])


    # # Apply the model to the incoming DataFrame
    # updated_df = updated_df.withColumn('s', lit(predict_speeding2(to_predict["engine_status"])))

    # updated_df = updated_df.withColumn('s', predict_speeding2(to_predict["engine_status"]))


    # Apply the model to the to_predict DataFrame
    # to_predict = to_predict.withColumn('s', predict_speeding2(to_predict["engine_status"]))


    # Apply the function to the list
    # prediction = predict_speeding2(to_predict["engine_status"])

    # Add the prediction as a new column to the updated_df DataFrame
    # updated_df = updated_df.withColumn('s', lit(prediction))

    # Join the original DataFrame with the predictions
    # updated_df = updated_df.join(to_predict, on=['thing_id'], how='left')
    # updated_df = updated_df.withColumn('is_speeding', lit(predict_speeding(to_predict['engine_status'],to_predict['power_supply_voltage'], to_predict['oil_value'], to_predict['fuel_liters'], to_predict['fuel_change'], to_predict['car_age'], to_predict['last_oil_change'])))
    # print(updated_df.show(2))
    




    # ==================================================
    # ==================================================
    # ==================================================
    # ==================================================
    # ==================================================
    # ==================================================
    # ==================================================
    # apply ml model on incomming eveents



    import joblib


    # Define a user-defined function that applies the model
    @udf(IntegerType())
    def predict_speeding(speed):
        model = load_model('pm_model.h5')

        # prediction = model.predict(np.array([[speed]]))
        return 1
        return int(prediction[0])
    # model = load_model('pm_model.h5')
    # 

        # Define a user-defined function that applies the model
    @udf(IntegerType())
    def mlp(engine_status, power_supply_voltage, oil_value, fuel_liters, fuel_change, car_age, last_oil_change):
        model = load_model('/Users/mac/Desktop/pm_model.h5')
        scaler = joblib.load('/Users/mac/Desktop/my_scaler.pkl')


        # prediction = model.predict(np.array([[engine_status], [power_supply_voltage], [oil_value], [fuel_liters], [fuel_change], [car_age], [last_oil_change]]))
        # return int(prediction[0])

            # Convert the inputs to a 2D array
        features = np.array([[engine_status, power_supply_voltage, oil_value, fuel_liters, fuel_change, car_age, last_oil_change]])
        
        # Scale the features using the saved scaler
        features_scaled = scaler.transform(features)
        
        # Reshape to 3D array (samples, timesteps, features)
        features_array = features_scaled.reshape((1, 1, features_scaled.shape[1]))
        
        # Apply the model
        prediction = model.predict(features_array)

 

        predicted_class = np.argmax(prediction)

        return int(predicted_class)
    
    # Apply the model to the incoming DataFrame
    updated_df = updated_df.withColumn('maintenance', mlp(updated_df['car_age'], updated_df['fuel_change'], updated_df['last_oil_change'], updated_df['power_supply_voltage'], updated_df['oil_value'], updated_df['fuel_liters'], updated_df['engine_status']))


    updated_df = updated_df.withColumn('fuel', lit(0.14))


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


# Now df includes a new column 'is_speeding' with the model's predictions



    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================

    # predict the rul of the car 




    @udf(FloatType())
    def predict_rul():

        # Load the model and scaler
        model = joblib.load('/Users/mac/Desktop/random_forest_model.joblib')
        scaler = joblib.load('/Users/mac/Desktop/scaler.joblib')


        features = np.array([[1599, 1.2, 0.3, 0.14, 0.5, 0.9, 0.7, 0.8, 0.9, 1.2, 1.1, 1.2, 1.3]])

        # Convert the inputs to a 2D array
        # features = np.array([[car_age, fuel_change, last_oil_change, power_supply_voltage, oil_value, fuel_liters, engine_status]])
        
        # Scale the features using the saved scaler
        features_scaled = scaler.transform(features)
        
        # Apply the model
        prediction = model.predict(features_scaled)

        return float(prediction[0])



    # Apply the model to the incoming DataFrame
    updated_df = updated_df.withColumn('rul', predict_rul())











    updated_df = updated_df.drop("last_oil_change", "car_age","fuel_change","power_supply_voltage","engine_status","oil_value","fuel_liters","fuel_percent")


    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================
    # =======================================================











    

    # # Define a function that applies the model
    # def predict_speeding(features):
    #     prediction = model.predict([features])
    #     return int(prediction[0])

    # Convert the PySpark DataFrame to a Pandas DataFrame
    # pandas_df = to_predict.toPandas()

    # Apply the model to the Pandas DataFrame
    # updated_df['is_speeding'] = pandas_df.apply(predict_speeding, axis=1)

    # Convert the Pandas DataFrame back to a PySpark DataFrame
    # updated_df = spark.createDataFrame(pandas_df)

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

