from pyspark.sql import SparkSession

def extract():
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

    # Extract the thing_id from the last row
    last_thing_id = df_last_row.select('thing_id').collect()[0][0]

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

    # Show the DataFrame
    print('this is a test')
    # df.show()
    # df.printSchema()
    # Perform further transformations or analysis on the DataFrame 

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
