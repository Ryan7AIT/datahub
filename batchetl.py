

from pyspark.sql import SparkSession

def extract():
    """
    Extracts data from MySQL and writes it to a PostgreSQL table using Spark.

    This function connects to a MySQL database, reads the last row from the `thing_dim` table,
    extracts data from the MySQL database based on the last row's `thing_id`, performs some transformations
    on the data, and writes it to a PostgreSQL table called `thing_dim`.

    Parameters:
        None

    Returns:
        None
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
        option('dbtable', '(SELECT thing_id FROM thing_dim ORDER BY thing_id DESC LIMIT 1) as last_row').\
        option('user', 'postgres').\
        option('password', 'ryqn').\
        load()

    # Check if df_last_row is not empty
    if df_last_row.count() > 0:
        # Extract the thing_id from the last row
        last_thing_id = df_last_row.select('thing_id').collect()[0][0]
        # Print the thing_id
        print('Last thing_id:', last_thing_id)
    else:
        last_thing_id = 0
        print('No data found in df_last_row')

    # Print the thing_id
    print('Last thing_id:', last_thing_id)

    df_mysq1 = spark.read.format('jdbc').\
        option('url', 'jdbc:mysql://localhost:3306').\
        option('driver', 'com.mysql.jdbc.Driver').\
        option('user', 'root').\
        option('password', '').\
        option('query', f"SELECT t.thing_id, tt.thing_type_designation, t.thing_designation, t.thing_matricule FROM geopfe.thing t, geopfe.thing_type tt WHERE t.thing_type_id = tt.thing_type_id AND t.thing_id > {last_thing_id}").\
        load()

    # Limit the DataFrame to 10 rows
    df = df_mysq1

    # rename some clounms to macth dw schema


    df = df.withColumnRenamed("thing_designation", "thing_name")
    df = df.withColumnRenamed("thing_type_designation", "thing_type")
    df = df.withColumnRenamed("thing_matricule", "thing_plate")

    # Write data to a PostgreSQL table
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/geopfe") \
        .option("dbtable", "thing_dim") \
        .option("user", "postgres") \
        .option("password", "ryqn") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    # Stop the SparkSession
    spark.stop()

# Test the extract function
extract()
