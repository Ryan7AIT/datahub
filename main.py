
from ast import List
from typing import Optional, Union
from cassandra.cluster import Cluster
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import mysql.connector
from pyspark.sql.functions import sum as _sum
from cassandra.query import dict_factory
import datetime
from pyspark.sql import functions as F

app = FastAPI()


spark = SparkSession.builder.appName("KafkaConsumer")\
.config("spark.jars", "/Users/mac/Downloads/postgresql-42.7.3.jar") \
.config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
.getOrCreate()


# Establish a connection to the PostgreSQL server
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe",
    user="postgres",
    password="ryqn"
)

# Establish a connection to the mysql server
# conn = mysql.connector.connect(
#     host="localhost",
#     port="3306",
#     database="geopfe",
#     user="root",
#     password=""
# )

# Create a cursor object for Mysql
cursor_mysql = conn.cursor()


# CORS configuration
origins = [
    "http://localhost:4200",  # Update with the origin of your Angular app
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Create a cursor object for PostgreSQL
cursor_postgres = conn.cursor()


@app.get("/")
def read_root():
    """
    Root endpoint of the API.

    Returns:
        dict: A dictionary with a "Hello" key and "World" value.
    """
    return {"Hello": "World"}


@app.get("/get_things")
def get_things():
    """
    Endpoint to get all things.

    Returns:
        list: A list of all things.
    """
    select_query = "SELECT * FROM thing_dim"
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    return rows


@app.get("/query/get_thing/{thing_id}")
def get_data(thing_id: int) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """
    select_query = f"""
    SELECT
        thing_id,
        thing_name
    FROM
        thing_dim
    WHERE

        thing_id = {thing_id}

LIMIT 1
 
    """
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    return rows

#http://localhost:8000/query/${thing_id}/distance
@app.get("/query/distancea")
def get_distance(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[List[int]]): List of years.
        months (Optional[List[str]]): List of months.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """




    # return ['11']
    select_query = f"""
        SELECT
            "year",
            SUM(v.travled_distance)
            FROM
            vehicle_peroformance v,
            thing_dim t,
            date_dim d
        WHERE
            v.date_id = d.date_id
            AND v.thing_id = t.thing_id
    """

    if thing_id :
        select_query += f" AND v.thing_id = {thing_id} group by \"year\""
    else:
        select_query += " group by \"year\""

    if years:

        select_query = f"""
            SELECT
            "month_name",
                SUM(v.travled_distance),
                "month"
            FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""
    
    if months:

        select_query = f"""
            SELECT
            TO_CHAR(d.full_date, 'Day'),
                SUM(v.travled_distance)
                "day"
            FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
            AND \"month\" = {months}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"day\", full_date"
        else:
            select_query += " group by \"day\", full_date"


    # Connect to Cassandra
    cluster = Cluster(['localhost'])
    session = cluster.connect()


    # Use the keyspace
    session.set_keyspace('pfe')
    session.row_factory = dict_factory

    # Query Cassandra for total_distance for each date

    if thing_id:
        cassandra_query = f" SELECT  SUM(active_time) as total_distance FROM vehicle_performance AND thing_id = {thing_id}"
    else:
        cassandra_query = " SELECT  SUM(active_time) as total_distance FROM vehicle_performance"

    rows = session.execute(cassandra_query)

    # Create two lists: one for the distances and one for the dates
    distances2 = [row['total_distance'] for row in rows]

    # Close connection
    session.shutdown()

    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()
    # Extracting years and distances into separate lists
    years = [entry[0] for entry in rows]
    distances = [entry[1] for entry in rows]

    distances[0] = distances[0] + distances2[0]
    # Creating a list containing years and distances lists
    result_list = [years, distances]
    return result_list




# get idle and active time
@app.get("/query/time")
def get_thing(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[List[int]]): List of years.
        months (Optional[List[str]]): List of months.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """




    # return ['11']
    select_query = f"""
        SELECT
            "year",
            SUM(v.idle_time),
            SUM(v.active_time)
            FROM
            vehicle_peroformance v,
            thing_dim t,
            date_dim d
        WHERE
            v.date_id = d.date_id
            AND v.thing_id = t.thing_id
    """

    if thing_id :
        select_query += f" AND v.thing_id = {thing_id} group by \"year\""
    else:
        select_query += " group by \"year\""

    if years:

        select_query = f"""
            SELECT
            "month_name",
                SUM(v.idle_time),
                SUM(v.active_time)
                FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""
    
    if months:

        select_query = f"""
            SELECT
            TO_CHAR(d.full_date, 'Day'),
                SUM(v.idle_time),
                SUM(v.active_time)
                "day"
            FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
            AND \"month\" = {months}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"day\", full_date"
        else:
            select_query += " group by \"day\", full_date"
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()
    # Extracting years and distances into separate lists
    years = [entry[0] for entry in rows]
    idle_time = [entry[1] for entry in rows]
    active_time = [entry[2] for entry in rows]
    # Creating a list containing years and distances lists
    result_list = [years, idle_time, active_time]
    return result_list


@app.get("/q")
def geta():
    return ['11']




# get speed
@app.get("/query/speed")
def get_speed(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[List[int]]): List of years.
        months (Optional[List[str]]): List of months.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """


    select_query = f"""
        SELECT
            "year",
            AVG(v.avg_speed),
            MAX(v.max_speed)
            FROM
            vehicle_peroformance v,
            thing_dim t,
            date_dim d
        WHERE
            v.date_id = d.date_id
            AND v.thing_id = t.thing_id
    """

    if thing_id :
        select_query += f" AND v.thing_id = {thing_id} group by \"year\""
    else:
        select_query += " group by \"year\""

    if years:

        select_query = f"""
            SELECT
            "month_name",
                AVG(v.avg_speed),
                MAX(v.max_speed)
                FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""
    
    if months:

        select_query = f"""
            SELECT
            TO_CHAR(d.full_date, 'Day'),
                AVG(v.avg_speed),
                MAX(v.max_speed),
                d.day
            FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND \"month\" = {months}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"day\", full_date"
        else:
            select_query += " group by full_date, d.day "
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()
    # Extracting years and distances into separate lists
    years = [entry[0] for entry in rows]
    avg_speed = [entry[1] for entry in rows]
    max_speed = [entry[2] for entry in rows]
    # Creating a list containing years and distances lists
    result_list = [years, avg_speed, max_speed]
    return result_list


# return all years
@app.get("/get_years")
def get_years():
    """
    Endpoint to get all years.

    Returns:
        list: A list of all years.
    """
    select_query = """
    
    SELECT
	DISTINCT
	"year"
FROM
	vehicle_peroformance v,
	thing_dim t,
	date_dim d
WHERE
	v.date_id = d.date_id
	AND v.thing_id = t.thing_id
	AND v.thing_id = 629
    """
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    return rows







# search for a thing by id
@app.get("/search/{thing_id}")
def search_thing(thing_id: str) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """


    select_query = f"""
    SELECT
        thing_id,
        thing_name
    FROM
        thing_dim t
    WHERE
  
     thing_name like '%{thing_id}%'
    GROUP BY
        t.thing_name,
        thing_id
    """
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    return rows



# get number of alerts
@app.get("/query/alerts")
def get_alerts(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[List[int]]): List of years.
        months (Optional[List[str]]): List of months.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """


    select_query = f"""
            SELECT
                f.alert_degree_id,
                COUNT(alert_count)
            FROM
                alert_fact f,
                alert_deg_dim d
            WHERE
                f.alert_degree_id = d.alert_degree_id
     
    """

    if thing_id :
        select_query += f" AND a.thing_id = {thing_id} group by f.alert_degree_id"
    else:
        select_query += " group by f.alert_degree_id"



    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    # extract alert_degree_id and alert_count into separate lists
    alert_degree_id = [entry[0] for entry in rows]
    alert_count = [entry[1] for entry in rows]

    # Creating a list containing alert_degree_id and alert_count lists
    result_list = [alert_degree_id, alert_count]
    return result_list

   


# get journey
@app.get("/query/journey")
def get_journey(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    select_query = f"""
        SELECT
            latitude,longitude
        FROM
            trace_week
        WHERE
            thing_id = 629
            AND engine_status = 1
            
        ORDER by trace_date
        """
    return [11]
    
    cursor_mysql.execute(select_query)
    rows = cursor_mysql.fetchall()
    return rows


# ==================================================================================
# real time


# define a route to get lastes real time data for a thing_id
@app.get("/realtime/{thing_id}")
def get_realtime_data(thing_id: str) -> Union[list, None]:
    """
    Endpoint to get real-time data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.

    Returns:
        list: A list of real-time data for the specified thing.
        None: If no real-time data is found for the specified thing.
    """

    # Connect to Cassandra cluster
    cluster = Cluster(['localhost'])
    session = cluster.connect()


    # Use the keyspace
    session.set_keyspace('pfe')

    select_query = f"""
    SELECT
        *
    FROM
        trace
    WHERE
        thing_id = '{thing_id}'
        
    LIMIT 1;
    """
    rows = session.execute(select_query)

        # Convert rows to a list of dictionaries
    data = []
    for row in rows:
        data.append({
            "thing_id": row[0],
            "timestamp": row[1],
            "status": row[2],
            "latitude": row[3],
            "longitude": row[4],
            "speed": row[5],
            "other_column": row[6]  # Add more columns as needed
        })

    # Close the connection
    session.shutdown()
    cluster.shutdown()

    return data









# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# testing routes


# define a route to get the avg speed for a thing_id

@app.get("/test")
def get_test_data(thing_id: Optional[int]=None) -> Union[list, None]:


    # .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\

    spark = SparkSession.builder.appName("KafkaConsumer")\
    .config("spark.jars", "/Users/mac/Downloads/postgresql-42.7.3.jar") \
    .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
    .getOrCreate()


    # select historcal data

    df_mysq1 = spark.read.format('jdbc').\
    option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
    option("driver", "org.postgresql.Driver").\
    option('user', 'postgres').\
    option('password', 'ryqn').\
    option('query', f"""
           
                SELECT
            
                        TO_CHAR(d.full_date, 'Day') ||
                        d.full_date as datee,
                        
                
                    SUM(v.travled_distance) as distance 
                FROM
                    vehicle_peroformance v,
                    thing_dim t,
                    date_dim d
                WHERE
                    v.date_id = d.date_id
                    AND v.thing_id = t.thing_id
                    AND year = 2024
                    AND month = 3
                    
                GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day')  

            """).\
    load()







    spark = SparkSession.builder.appName("KafkaConsumer")\
    .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0')\
    .getOrCreate()



    # Load the Cassandra table into a DataFrame
    cassandra_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="vehicle_performance", keyspace="pfe") \
        .load()
    


    if(thing_id):
        cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == ' 629')

    else:
        cassandra_df = cassandra_df.groupBy("full_date").sum("traveled_distance")

    cassandra_df = cassandra_df.select("full_date", 'traveled_distance')


    print(cassandra_df.show())

    result = df_mysq1.union(cassandra_df)

    print(result.show())


    r = [row[0] for row in result.collect()]
    r2 = [row[1] for row in result.collect()]


    
    return [r,r2]
    return cassandra_df.agg(_sum("traveled_distance")).collect()





# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# ----------------------------------------------------------
# both storage with pyspark!!!


@app.get("/query/distancea2")
def get_distance(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[List[int]]): List of years.
        months (Optional[List[str]]): List of months.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """

    
    if months:


        print('months number')
        # Get the current date
        current_date = datetime.datetime.now()

        # Extract the month number from the current date
        month_number = current_date.month
        print(month_number)
        print(months)

        if (months ==  str(month_number)):
            print('guiguiguguigui')

            print('yeyeyyeye')





        # select todays data
            




            # Load the Cassandra table into a DataFrame
            cassandra_df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="vehicle_performance", keyspace="pfe") \
                .load()
            

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.sum("traveled_distance").alias("traveled_distance"))

            
            cassandra_df = cassandra_df.select("full_date", 'traveled_distance')

            if thing_id:

                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            
                                SUM(v.travled_distance) as distance 
                            FROM
                                vehicle_peroformance v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                    AND v.thing_id = {thing_id}
                                
                            GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day')  

                        """).\
                load()
            else:
                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            
                                SUM(v.travled_distance) as distance 
                            FROM
                                vehicle_peroformance v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                                
                            GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day')  

                        """).\
                load()


            result = df_mysq1.union(cassandra_df)



            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]

          

            
            return [r,r2]
        else:
            select_query = f"""
            SELECT
            TO_CHAR(d.full_date, 'Day'),
                SUM(v.travled_distance)
                "day"
            FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
            AND \"month\" = {months}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"day\", full_date"
        else:
            select_query += " group by \"day\", full_date"

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        # Creating a list containing years and distances lists
        result_list = [years, distances]
        return result_list


    elif years:

        select_query = f"""
            SELECT
            "month_name",
                SUM(v.travled_distance),
                "month"
            FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        # Creating a list containing years and distances lists
        result_list = [years, distances]
        return result_list


    else:

        select_query = f"""
            SELECT
                "year",
                SUM(v.travled_distance)
                FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
        """

        if thing_id :
            select_query += f" AND v.thing_id = {thing_id} group by \"year\""
        else:
            select_query += " group by \"year\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        # Creating a list containing years and distances lists
        result_list = [years, distances]
        return result_list




@app.get("/query/time2")
def get_thing(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[List[int]]): List of years.
        months (Optional[List[str]]): List of months.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """


    if months:


        current_date = datetime.datetime.now()

        # Extract the month number from the current date
        month_number = current_date.month

        if (months ==  str(month_number)):
        




            # Load the Cassandra table into a DataFrame
            cassandra_df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="vehicle_performance", keyspace="pfe") \
                .load()
            
            print(cassandra_df.show())
            

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.sum("active_time").alias("active_time"), F.sum("idle_time").alias("idle_time"))



            
            cassandra_df = cassandra_df.select("full_date", 'active_time','idle_time')

            print(cassandra_df.show())

            if thing_id:

                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            
                                SUM(v.idle_time) as idle_time,
                                SUM(v.active_time) as active_time
                            FROM
                                vehicle_peroformance v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = 2024
                                AND month = 3
                        AND v.thing_id = {thing_id}
                                    
                                GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                """
                        ).\
                load()
            else:

                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            
                                SUM(v.idle_time) as idle_time,
                                SUM(v.active_time) as active_time
                            FROM
                                vehicle_peroformance v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = 2024
                                AND month = 3
                                    
                                GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                """
                        ).\
                load()

            result = df_mysq1.union(cassandra_df)


            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]

                # Stop the SparkSession
            # spark.stop()
            
            return [r,r2]
        

        else:
            select_query = f"""

            SELECT
	TO_CHAR(d.full_date, 'Day') || d.full_date AS datee,
	SUM(v.idle_time),
	SUM(v.active_time) "day"
FROM
	vehicle_peroformance v,
	thing_dim t,
	date_dim d
WHERE
	v.date_id = d.date_id
	AND v.thing_id = t.thing_id
	AND year = {years}
	AND month = {months}
GROUP BY
	TO_CHAR(d.full_date, 'Day'), d.full_date;
        """

            
        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        # Creating a list containing years and distances lists
        result_list = [years, distances]
        return result_list
            
    
    elif years:
        select_query = f"""
            SELECT
            "month_name",
                SUM(v.idle_time),
                SUM(v.active_time)
                FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        # Creating a list containing years and distances lists
        result_list = [years, distances]
        return result_list

    else:
    

        # return ['11']
        select_query = f"""
            SELECT
                "year",
                SUM(v.idle_time),
                SUM(v.active_time)
                FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
        """

        if thing_id :
            select_query += f" AND v.thing_id = {thing_id} group by \"year\""
        else:
            select_query += " group by \"year\""



        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        # Creating a list containing years and distances lists
        result_list = [years, distances]
        return result_list




@app.get("/query/speed2")
def get_speed(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[List[int]]): List of years.
        months (Optional[List[str]]): List of months.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of data for the specified thing.
        None: If no data is found for the specified thing.
    """


    if months:


        current_date = datetime.datetime.now()

        # Extract the month number from the current date
        month_number = current_date.month

        if (months ==  str(month_number)):
        




            # Load the Cassandra table into a DataFrame
            cassandra_df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="vehicle_performance", keyspace="pfe") \
                .load()
            
            print(cassandra_df.show())
            

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.avg("avg_speed").alias("avg_speed"), F.max("max_speed").alias("max_speed"))



            
            cassandra_df = cassandra_df.select("full_date", 'avg_speed','max_speed')

            print(cassandra_df.show())

            if thing_id:

                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            
                                AVG(v.avg_speed) as avg_speed,
                                MAX(v.max_speed) as max_speed
                            FROM
                                vehicle_peroformance v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND v.thing_id = {thing_id}
                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                ).load()
            else:
                    
                    df_mysq1 = spark.read.format('jdbc').\
                    option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                    option("driver", "org.postgresql.Driver").\
                    option('user', 'postgres').\
                    option('password', 'ryqn').\
                    option('query', f"""
                        
                                SELECT
                            
                                        TO_CHAR(d.full_date, 'Day') ||
                                        d.full_date as datee,
                                        
                                
                                    AVG(v.avg_speed) as avg_speed,
                                    MAX(v.max_speed) as max_speed
                                FROM
                                    vehicle_peroformance v,
                                    thing_dim t,
                                    date_dim d
                                WHERE
                                    v.date_id = d.date_id
                                    AND v.thing_id = t.thing_id
                                    AND year = {years}
                                    AND month = {months}
                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                    ).load()

            result = df_mysq1.union(cassandra_df)


            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]
            r3 = [row[1] for row in result.collect()]


                # Stop the SparkSession
            # spark.stop()
            
            return [r,r2,r3]
        
        else:
            select_query = f"""

            SELECT 
                TO_CHAR(d.full_date, 'Day') || d.full_date AS datee,
                AVG(v.avg_speed),
                MAX(v.max_speed),
                d.day
            FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND \"month\" = {months}
            GROUP BY
                \"day\", full_date
            """ 

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        max_speed = [entry[2] for entry in rows]


        # Creating a list containing years and distances lists
        result_list = [years, distances,max_speed]
        return result_list
    
    elif years:
        select_query = f"""
            SELECT
            "month_name",
                AVG(v.avg_speed),
                MAX(v.max_speed)
                FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        max_speed = [entry[2] for entry in rows]


        # Creating a list containing years and distances lists
        result_list = [years, distances,max_speed]
        return result_list
    else:
        select_query = f"""
            SELECT
                "year",
                AVG(v.avg_speed),
                MAX(v.max_speed)
                FROM
                vehicle_peroformance v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
        """

        if thing_id :
            select_query += f" AND v.thing_id = {thing_id} group by \"year\""
        else:
            select_query += " group by \"year\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        max_speed = [entry[2] for entry in rows]


        # Creating a list containing years and distances lists
        result_list = [years, distances,max_speed]
        return result_list

        


# UPDATE vehicle_performance
# SET avg_speed = 56, max_speed = 89
# WHERE thing_id = ' 629';

# INSERT INTO vehicle_performance (thing_id, active_time, idle_time) VALUES ('338', 10,11);
    

    