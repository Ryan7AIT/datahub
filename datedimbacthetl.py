

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def extract():
    """
    Extracts data from MySQL and writes it to a PostgreSQL table for date dimmnsion.
    """
   
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("MySQL Extraction") \
        .config("spark.jars", "/Users/mac/Downloads/mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar, /Users/mac/Downloads/postgresql-42.7.3.jar") \
        .getOrCreate()

    # Read the last row from the thing_dim table
    df_last_row = spark.read.format('jdbc').\
        option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
        option('driver', 'org.postgresql.Driver').\
        option('dbtable', '(SELECT date_id FROM date_dim ORDER BY date_id DESC LIMIT 1) as last_row').\
        option('user', 'postgres').\
        option('password', 'ryqn').\
        load()

    # Check if df_last_row is not empty
    if df_last_row.count() > 0:
        # Extract the thing_id from the last row
        last_thing_id = df_last_row.select('date_id').collect()[0][0]
    else:
        last_thing_id = 0
        print('No data found in df_last_row')
    # Print the thing_id
        


    df_mysq1 = spark.read.format('jdbc').\
        option('url', 'jdbc:mysql://localhost:3306').\
        option('driver', 'com.mysql.jdbc.Driver').\
        option('user', 'root').\
        option('password', '').\
        option('query', f"SELECT DISTINCT date_insertion_day AS date_id, date_insertion_day AS full_date, YEAR(date_insertion_day) AS year, MONTH(date_insertion_day) AS month, DAY(date_insertion_day) AS day, DATE_FORMAT(date_insertion_day, '%Y-%m') AS month_year, DATE_FORMAT(date_insertion_day, '%M') AS month_name, CONCAT(YEAR(date_insertion_day), '-Q', QUARTER(date_insertion_day)) AS quarter, CASE WHEN DAYOFWEEK(date_insertion_day) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type, CASE WHEN MONTH(date_insertion_day) IN (1, 2, 12) THEN 'Winter' WHEN MONTH(date_insertion_day) IN (3, 4, 5) THEN 'Spring' WHEN MONTH(date_insertion_day) IN (6, 7, 8) THEN 'Summer' ELSE 'Fall' END AS season FROM geopfe.trace_week where date_insertion_day > {last_thing_id}").\
        load()


    # Limit the DataFrame to 10 rows
    df = df_mysq1

    # rename some clounms to macth dw schema
    
    # Add a new column 'row_id' with monotonically increasing values
    df = df.withColumn('row_id', monotonically_increasing_id() + 1)

    # Drop the 'date_id' column
    df = df.drop('date_id')

    # Rename the 'row_id' column to 'date_id'
    df = df.withColumnRenamed('row_id', 'date_id')

    df.head()

    # Write data to a PostgreSQL table
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/geopfe") \
        .option("dbtable", "date_dim") \
        .option("user", "postgres") \
        .option("password", "ryqn") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    # Stop the SparkSession
    spark.stop()

# Test the extract function
extract()
