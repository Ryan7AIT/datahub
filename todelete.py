from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Connection") \
    .config("spark.jars", "//Users/mac/Downloads/postgresql-42.7.3.jar") \
    .getOrCreate()




# Create a simple DataFrame
data = [("John", 25), ("Alice", 30), ("Bob", 35)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Write data to a PostgreSQL table
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/geopfe") \
    .option("dbtable", "new_table") \
    .option("user", "postgres") \
    .option("password", "ryqn") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()



# Close the SparkSession
spark.stop()