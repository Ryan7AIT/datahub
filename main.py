
from ast import List
from typing import Optional, Union
from cassandra.cluster import Cluster
from fastapi import FastAPI, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import mysql.connector
from pyspark.sql.functions import sum as _sum
from cassandra.query import dict_factory
import datetime
from pyspark.sql import functions as F
from geopy.geocoders import Nominatim
import math
# from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from jose import JWTError, jwt
from passlib.context import CryptContext
from typing import Optional

def get_street_address(p):
    latitude , longitude = p.split(',')
    geolocator = Nominatim(user_agent="my_app")
    location = geolocator.reverse((latitude, longitude))
    address = location.raw['address']

    return address.get('town'), address.get('state'), address.get('country')


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

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["GET", "POST", "PUT", "DELETE"],
#     allow_headers=["*"],
# )

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
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
async def get_distance(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
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
async def get_thing(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
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
async def get_speed(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
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
async def get_years():
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
async def search_thing(thing_id: str) -> Union[list, None]:
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
        thing_designation
    FROM
        thing_dim t
    WHERE
  
     thing_designation like '%{thing_id}%'
    GROUP BY
        t.thing_designation,
        thing_id
    """
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    return rows



# get number of alerts
@app.get("/query/alerts")
async def get_alerts(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
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
                alert_deg_dim d,
                thing_dim t
            WHERE
                f.alert_degree_id = d.alert_degree_id

                AND f.thing_id = t.thing_id

     
    """

    if thing_id :
        select_query += f" AND t.thing_id = {thing_id} group by f.alert_degree_id"

    elif group_id:
        select_query += f" AND t.group_id = {group_id} group by f.alert_degree_id"
    elif type_id:
        select_query += f" AND t.type_id = {type_id} group by f.alert_degree_id"

    
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
async def get_journey(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
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
# ==================================================================================
# ==================================================================================
# ==================================================================================
# ==================================================================================
# ==================================================================================
# ==================================================================================
# ==================================================================================
# ==================================================================================
# ==================================================================================
# real time


# define a route to get lastes real time data for a thing_id
@app.get("/realtime/{thing_id}")
async def get_realtime_data(thing_id: str) -> Union[list, None]:
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
            "thing_id": row.thing_id,
            "timestamp": row.trace_date,
            "status": row.engine_status,
            "fuel_liters": row.fuel_liters,
            "fuel_percent": row.fuel_percent,
            "latitude": row.lat,
            "longitude": row.long,
            "speed": row.speed,
            "oil_value": row.oil_value,
            "battery": row.battery
        })

    # Close the connection
    session.shutdown()
    cluster.shutdown()

    return data


# define a route to get all cars
@app.get("/get_cars")
async def get_cars(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None):
    """
    Endpoint to get all cars.

    Returns:
        list: A list of all cars.
    """

    # Connect to Cassandra cluster
    cluster = Cluster(['localhost'])
    session = cluster.connect()


    # Use the keyspace
    session.set_keyspace('pfe')

    if (thing_id):

        select_query = f"""
    SELECT
        *
    FROM
        vehicle_performance

    where thing_id={thing_id}
        """     

    elif (type_id):
             
            select_query = f"""
    SELECT
        *
    FROM
        vehicle_performance

    where type_id={type_id}
ALLOW FILTERING
        """

        

    elif (group_id):
             
            select_query = f"""
    SELECT
        *
    FROM
        vehicle_performance

    where group_id={group_id}
ALLOW FILTERING

        """

    else:
        select_query = f"""
    SELECT
        *
    FROM
        vehicle_performance
        """
    rows = session.execute(select_query)


        # Close the connection
    session.shutdown()
    cluster.shutdown()

    result = []
    for row in rows:
        result.append({
            "latitude": row.latitude,
            "longitude": row.longitude,
            "thing_id": row.thing_id,
            "active_time": row.active_time,
            "avg_speed": row.avg_speed,
            "max_speed": row.max_speed
        })
    return result


# define a route to get all cars
@app.get("/get_car2")
async def get_cars(thing_id: str):
    """
    Endpoint to get all cars.

    Returns:
        list: A list of all cars.
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
        vehicle_performance

    where thing_id={thing_id}
        """
    rows = session.execute(select_query)


        # Close the connection
    session.shutdown()
    cluster.shutdown()

    result = []
    for row in rows:
        result.append({
            "latitude": row.latitude,
            "longitude": row.longitude,
            "thing_id": row.thing_id,
            "active_time": row.active_time,
            "avg_speed": row.avg_speed,
            "max_speed": row.max_speed,
            "prediction": row.maintenance,
            "rul": math.ceil(row.rul) 
        })
    return result


# define a route to get a car
@app.get("/get_car/{thing_id}")
async def get_car(thing_id: str) -> Union[list, None]:
    """
    Endpoint to get a specific car.

    Args:
        thing_id (int): The ID of the car.

    Returns:
        list: A list of the specified car.
        None: If no car is found for the specified ID.
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
        thing_id = {thing_id}
        
    LIMIT 1;
    """
    rows = session.execute(select_query)

        # Convert rows to a list of dictionaries
    data = []
    for row in rows:
        data.append({
            "thing_id": row.thing_id,
            "timestamp": row.trace_date,
            # "status": row.status,
            "latitute": row.latitute,
            "longitude": row.longitude,
            "speed": row.speed
            # "other_column": row.other_column  # Add more columns as needed
        })

    # Close the connection
    session.shutdown()
    cluster.shutdown()

    return data



# get thing types
@app.get("/get_thing_types")
async def get_thing_types():
    """
    Endpoint to get all thing types.

    Returns:
        list: A list of all thing types.
    """
    select_query = """
SELECT DISTINCT
	type_id,
	thing_type_designation
FROM
	thing_dim
    """
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    # retutn a dict
    data = []
    for row in rows:
        data.append({
            "type_id": row[0],
            "thing_type_designation": row[1]
        })


    return data

# get thing groups
@app.get("/get_thing_groups")
async def get_thing_groups():
    """
    Endpoint to get all thing groups.

    Returns:
        list: A list of all thing groups.
    """
    select_query = """
SELECT DISTINCT
    group_id,
    group_name
    FROM
    thing_dim
    limit 10
    """
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    # retutn a dict
    data = []
    for row in rows:
        data.append({
            "group_id": row[0],
            "thing_group_designation": row[1]
        })


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
async def get_test_data(thing_id: Optional[int]=None) -> Union[list, None]:


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
async def get_distance(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None,  years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
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


        # Get the current date
        current_date = datetime.datetime.now()

        # Extract the month number from the current date
        month_number = current_date.month
     

        if (months ==  str(month_number)):

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

            elif group_id:
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
                    AND t.group_id = {group_id}
                                    
                                GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day')  
    
                            """).load()
                
            elif type_id:
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
                    AND t.type_id = {type_id}
                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day')  
        
                                """).load()

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
        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"day\", full_date"
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"day\", full_date"
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
        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"month\", \"month_name\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"month\", \"month_name\""
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
        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"year\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"year\""
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
async def get_thing(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
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
            
            

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.sum("active_time").alias("active_time"), F.sum("idle_time").alias("idle_time"))



            
            cassandra_df = cassandra_df.select("full_date", 'active_time','idle_time')


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
                                AND year = {years}
                                AND month = {months}
                        AND v.thing_id = {thing_id}
                                    
                                GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                """
                        ).\
                load()

            elif group_id:
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
                                AND year = {years}
                                AND month = {months}
                                       AND t.group_id = {group_id}

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
                                AND year = {years}
                                AND month = {months}
                                    
                                GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                """
                        ).\
                load()

            result = df_mysq1.union(cassandra_df)


            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]
            r3 = [row[2] for row in result.collect()]


                # Stop the SparkSession
            # spark.stop()
            
            return [r,r2,r3]
        

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
                
                        """

            if thing_id:
                select_query += f" AND v.thing_id = {thing_id} group by TO_CHAR(d.full_date, 'Day'), d.full_date"
            elif group_id:
                select_query += f" AND t.group_id = {group_id} group by TO_CHAR(d.full_date, 'Day'), d.full_date"
            elif type_id:
                select_query += f" AND t.type_id = {type_id} group by TO_CHAR(d.full_date, 'Day'), d.full_date"
            else:
                select_query += " group by TO_CHAR(d.full_date, 'Day'), d.full_date"
            
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

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"month\", \"month_name\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"month\", \"month_name\""

        else:
            select_query += " group by \"month\", \"month_name\""



        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        distances2 = [entry[2] for entry in rows]

        # Creating a list containing years and distances lists
        result_list = [years, distances,distances]
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


        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"year\""
        
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"year\""

        else:
            select_query += " group by \"year\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        distances2 = [entry[2] for entry in rows]


        # Creating a list containing years and distances lists
        result_list = [years, distances,distances2]
        return result_list




@app.get("/query/speed2")
async def get_speed(thing_id: Optional[int]=None, group_id: Optional[int]=None,  type_id: Optional[int]=None,    years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
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
            
            

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.avg("avg_speed").alias("avg_speed"), F.max("max_speed").alias("max_speed"))



            
            cassandra_df = cassandra_df.select("full_date", 'avg_speed','max_speed')


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

            elif group_id:
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
                            AND t.group_id = {group_id}
                                        GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                            
                                        """
                    ).load()


            elif type_id:
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
                        AND t.type_id = {type_id}
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
            """ 

            if thing_id:
                select_query += f" AND v.thing_id = {thing_id} group by \"day\", full_date"
            elif group_id:
                select_query += f" AND t.group_id = {group_id} group by \"day\", full_date"
            elif type_id:
                select_query += f" AND t.type_id = {type_id} group by \"day\", full_date"
            else:
                select_query += " group by \"day\", full_date"
                

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

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"month\", \"month_name\""

        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"month\", \"month_name\""
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

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"year\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"year\""
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
    




# get fuel stat
@app.get('/query/fuel2')
async def get_fuel(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get fuel data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[str]): The year.
        months (Optional[str]): The month.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of fuel data for the specified thing.
        None: If no fuel data is found for the specified thing.
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
            
            

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.sum("fuel").alias("fuel_consumed"))



            
            cassandra_df = cassandra_df.select("full_date", 'fuel_consumed')


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
                                    
                            
                                SUM(v.fuel) as fuel_consumed
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
                ).\
                load()


            elif group_id:
                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            
                                SUM(v.fuel) as fuel_consumed
                            FROM
                                vehicle_peroformance v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND t.group_id = {group_id}
                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                ).\
                load()

            elif type_id:
                                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            
                                SUM(v.fuel) as fuel_consumed
                            FROM
                                vehicle_peroformance v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND t.type_id = {type_id}
                                        
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
                                        
                                
                                    SUM(v.fuel) as fuel_consumed
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
                    ).\
                    load()

            result = df_mysq1.union(cassandra_df)
            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]

            return [r,r2]
        
        else:
            select_query = f"""

            SELECT 
                TO_CHAR(d.full_date, 'Day') || d.full_date AS datee,
                SUM(v.fuel),
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
                select_query += f" AND v.thing_id = {thing_id} group by \"day\", d.full_date"
            elif group_id:
                select_query += f" AND t.group_id = {group_id} group by \"day\", d.full_date"
            elif type_id:
                select_query += f" AND t.type_id = {type_id} group by \"day\", d.full_date"

            else :
                select_query += " group by \"day\", d.full_date"
        
        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        return [years, distances]
    
    elif years:
        select_query = f"""
            SELECT
            "month_name",
                SUM(v.fuel)
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

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"month\", \"month_name\""

        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        return [years, distances]
    
    else:
        select_query = f"""
            SELECT
                "year",
                SUM(v.fuel)
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

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"year\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"year\""
        else:
            select_query += " group by \"year\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]

        return [years, distances]

                           



#get alert stats
@app.get('/query/alert2')
async def get_alert(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get alert data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[str]): The year.
        months (Optional[str]): The month.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of alert data for the specified thing.
        None: If no alert data is found for the specified thing.
    """

    if months:

        current_date = datetime.datetime.now()

        # Extract the month number from the current date
        month_number = current_date.month

        if (months ==  str(month_number)):
        




            # Load the Cassandra table into a DataFrame
            cassandra_df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="alert", keyspace="pfe") \
                .load()
            
            # select alert with type 1
            cassandra_df = cassandra_df.filter(cassandra_df['type_id'] == 1)

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            if(group_id):
                cassandra_df = cassandra_df.filter(cassandra_df['group_id'] == str(group_id))            

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.sum("alert_number").alias("alert_number"))



            
            cassandra_df = cassandra_df.select("full_date", 'alert_number')


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
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                                AND v.thing_id = {thing_id}
                                AND alert_degree_id = 1

                                        
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
                                        
                                
                                    COUNT(*) AS alerts_raised
                                FROM
                                    alert_fact v,
                                    thing_dim t,
                                    date_dim d
                                WHERE
                                    v.date_id = d.date_id
                                    AND v.thing_id = t.thing_id
                                    AND year = {years}
                                    AND month = {months}
                                    AND alert_degree_id = 1

                                            
                                        GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                        """ 
                    ).\
                    load()

            result = df_mysq1.union(cassandra_df)
            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]

            return [r,r2]
        
        else:
            select_query = f"""

            SELECT 
                TO_CHAR(d.full_date, 'Day') || d.full_date AS datee,
                COUNT(*),
                d.day
            FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND \"month\" = {months}
                AND alert_degree_id = 1
            GROUP BY
                \"day\", full_date
            """

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    
    elif years:
        select_query = f"""
            SELECT
            "month_name",
                COUNT(*)
                FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND alert_degree_id = 1
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"month\", \"month_name\""

        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    
    else:
        select_query = f"""
            SELECT
                "year",
                COUNT(*)
                FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND alert_degree_id = 1

        """

        if thing_id :
            select_query += f" AND v.thing_id = {thing_id} group by \"year\""

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"year\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"year\""
            # return [11]
        else:
            select_query += " group by \"year\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]


#get alert stats
@app.get('/query/alert2/t1')
async def get_alert(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get alert data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[str]): The year.
        months (Optional[str]): The month.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of alert data for the specified thing.
        None: If no alert data is found for the specified thing.
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
            
            

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.sum("alerts_raised").alias("alerts_raised"))



            
            cassandra_df = cassandra_df.select("full_date", 'alerts_raised')


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
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
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
                ).\
                load()

            elif group_id:
                                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND t.group_id = {group_id}
                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                ).\
                load()
                                
            elif type_id:
                                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND t.type_id = {type_id}
                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                ).\
                load()
            else:
                    
                    df_mysq1 = spark.read.format('jdbc').\
                    option
                    ('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                    option("driver", "org.postgresql.Driver").\
                    option('user', 'postgres').\
                    option('password', 'ryqn').\
                    option('query', f"""
                            SELECT
                            
                                        TO_CHAR(d.full_date, 'Day') ||
                                        d.full_date as datee,
                                        
                                
                                    COUNT(*) AS alerts_raised
                                FROM
                                    alert_fact v,
                                    thing_dim t,
                                    date_dim d
                                WHERE
                                    v.date_id = d.date_id
                                    AND v.thing_id = t.thing_id
                                    AND year = {years}
                                    AND month = {months}
                                            
                                        GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                        """ 
                    ).\
                    load()

            result = df_mysq1.union(cassandra_df)
            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]

            return [r,r2]
        
        else:
            select_query = f"""

            SELECT 
                TO_CHAR(d.full_date, 'Day') || d.full_date AS datee,
                COUNT(*),
                d.day
            FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND \"month\" = {months}
                AND alert_degree_id = 1

            """

            if thing_id:
                select_query += f" AND v.thing_id = {thing_id} group by \"day\", full_date"
            elif group_id:
                select_query += f" AND t.group_id = {group_id} group by \"day\", full_date"
            elif type_id:
                select_query += f" AND t.type_id = {type_id} group by \"day\", full_date"

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    
    elif years:
        select_query = f"""
            SELECT
            "month_name",
                COUNT(*)
                FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND alert_degree_id = 1
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"month\", \"month_name\""

        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    
    else:
        select_query = f"""
            SELECT
                "year",
                COUNT(*)
                FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND alert_degree_id = 1

        """

        if thing_id :
            select_query += f" AND v.thing_id = {thing_id} group by \"year\""

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"year\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"year\""
        else:
            select_query += " group by \"year\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]


#get alert stats
@app.get('/query/alert2/t2')
async def get_alert(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get alert data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[str]): The year.
        months (Optional[str]): The month.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of alert data for the specified thing.
        None: If no alert data is found for the specified thing.
    """

    if months:

        current_date = datetime.datetime.now()

        # Extract the month number from the current date
        month_number = current_date.month

        if (months ==  str(month_number)):
        




            # Load the Cassandra table into a DataFrame
            cassandra_df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="alert", keyspace="pfe") \
                .load()
            
            # select alert with type 1
            cassandra_df = cassandra_df.filter(cassandra_df['type_id'] == 2)

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            if(group_id):
                cassandra_df = cassandra_df.filter(cassandra_df['group_id'] == str(group_id))            

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.sum("alert_number").alias("alert_number"))



            
            cassandra_df = cassandra_df.select("full_date", 'alert_number')


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
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND v.thing_id = {thing_id}
                        AND alert_degree_id = 2
                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                ).\
                load()

            elif group_id:
                                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND t.group_id = {group_id}
                        AND alert_degree_id = 2

                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                ).\
                load()
                                
            elif type_id:
                                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND t.type_id = {type_id}
                        AND alert_degree_id = 2

                                        
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
                                        
                                
                                    COUNT(*) AS alerts_raised
                                FROM
                                    alert_fact v,
                                    thing_dim t,
                                    date_dim d
                                WHERE
                                    v.date_id = d.date_id
                                    AND v.thing_id = t.thing_id
                                    AND year = {years}
                                    AND month = {months}
                        AND alert_degree_id = 2
                                            
                                        GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                        """ 
                    ).\
                    load()

            result = df_mysq1.union(cassandra_df)
            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]

            return [r,r2]
        
        else:
            select_query = f"""

            SELECT 
                TO_CHAR(d.full_date, 'Day') || d.full_date AS datee,
                COUNT(*),
                d.day
            FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND \"month\" = {months}
                AND alert_degree_id = 2
 
            """

            if thing_id:
                select_query += f" AND v.thing_id = {thing_id} group by \"day\", full_date"
            elif group_id:
                select_query += f" AND t.group_id = {group_id} group by \"day\", full_date"
            elif type_id:
                select_query += f" AND t.type_id = {type_id} group by \"day\", full_date"
            else:
                select_query += " group by \"day\", full_date"

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    
    elif years:
        select_query = f"""
            SELECT
            "month_name",
                COUNT(*)
                FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND alert_degree_id = 2
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"month\", \"month_name\""

        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    
    else:
        select_query = f"""
            SELECT
                "year",
                COUNT(*)
                FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND alert_degree_id = 2

        """

        if thing_id :
            select_query += f" AND v.thing_id = {thing_id} group by \"year\""

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"year\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"year\""
        else:
            select_query += " group by \"year\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    


#get alert stats
@app.get('/query/alert2/t3')
async def get_alert(thing_id: Optional[int]=None, group_id: Optional[int]=None, type_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    Endpoint to get alert data for a specific thing.

    Args:
        thing_id (int): The ID of the thing.
        years (Optional[str]): The year.
        months (Optional[str]): The month.
        d1 (Optional[str]): Start date.
        d2 (Optional[str]): End date.

    Returns:
        list: A list of alert data for the specified thing.
        None: If no alert data is found for the specified thing.
    """

    if months:

        current_date = datetime.datetime.now()

        # Extract the month number from the current date
        month_number = current_date.month

        if (months ==  str(month_number)):
        



            # Load the Cassandra table into a DataFrame
            cassandra_df = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="alert", keyspace="pfe") \
                .load()
            
            # select alert with type 1
            cassandra_df = cassandra_df.filter(cassandra_df['type_id'] == 3)

            if(thing_id):
                cassandra_df = cassandra_df.filter(cassandra_df['thing_id'] == str(thing_id))

            if(group_id):
                cassandra_df = cassandra_df.filter(cassandra_df['group_id'] == str(group_id))            

            else:
                cassandra_df = cassandra_df.groupBy("full_date").agg(F.sum("alert_number").alias("alert_number"))



            
            cassandra_df = cassandra_df.select("full_date", 'alert_number')

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
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND v.thing_id = {thing_id}
                        AND alert_degree_id = 3
                                        
                                    GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                ).\
                load()

            elif group_id:
                                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND t.group_id = {group_id}
                        AND alert_degree_id = 3

                                        
                                    GROUP by   d.full_date, TO_CHAR(d.full_date, 'Day') 
                                    """
                ).\
                load()
                                
            elif type_id:
                                df_mysq1 = spark.read.format('jdbc').\
                option('url', 'jdbc:postgresql://localhost:5432/geopfe').\
                option("driver", "org.postgresql.Driver").\
                option('user', 'postgres').\
                option('password', 'ryqn').\
                option('query', f"""
                    
                            SELECT
                        
                                    TO_CHAR(d.full_date, 'Day') ||
                                    d.full_date as datee,
                                    
                            COUNT(*) AS alerts_raised
                            FROM
                                alert_fact v,
                                thing_dim t,
                                date_dim d
                            WHERE
                                v.date_id = d.date_id
                                AND v.thing_id = t.thing_id
                                AND year = {years}
                                AND month = {months}
                        AND t.type_id = {type_id}
                        AND alert_degree_id = 3

                                        
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
                                        
                                
                                    COUNT(*) AS alerts_raised
                                FROM
                                    alert_fact v,
                                    thing_dim t,
                                    date_dim d
                                WHERE
                                    v.date_id = d.date_id
                                    AND v.thing_id = t.thing_id
                                    AND year = {years}
                                    AND month = {months}
                        AND alert_degree_id = 3

                                            
                                        GROUP by             d.full_date, TO_CHAR(d.full_date, 'Day') 
                                        """ 
                    ).\
                    load()

            result = df_mysq1.union(cassandra_df)
            r = [row[0] for row in result.collect()]
            r2 = [row[1] for row in result.collect()]

            return [r,r2]
        
        else:
            select_query = f"""

            SELECT 
                TO_CHAR(d.full_date, 'Day') || d.full_date AS datee,
                COUNT(*),
                d.day
            FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND \"month\" = {months}
                AND alert_degree_id = 3
            """

            if thing_id:
                select_query += f" AND v.thing_id = {thing_id} group by \"day\", full_date"
            elif group_id:
                select_query += f" AND t.group_id = {group_id} group by \"day\", full_date"
            elif type_id:
                 select_query += f" AND t.type_id = {type_id} group by \"day\", full_date"

            else:
                select_query += " group by \"day\", full_date"
                 

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    
    elif years:
        select_query = f"""
            SELECT
            "month_name",
                COUNT(*)
                FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND \"year\" = {years}
                AND alert_degree_id = 3
        """

        if thing_id:
            select_query += f" AND v.thing_id = {thing_id} group by \"month\", \"month_name\""

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"month\", \"month_name\""

        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"month\", \"month_name\""
        else:
            select_query += " group by \"month\", \"month_name\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]
    
    else:
        select_query = f"""
            SELECT
                "year",
                COUNT(*)
                FROM
                alert_fact v,
                thing_dim t,
                date_dim d
            WHERE
                v.date_id = d.date_id
                AND v.thing_id = t.thing_id
                AND alert_degree_id = 3

        """

        if thing_id :
            select_query += f" AND v.thing_id = {thing_id} group by \"year\""

        elif group_id:
            select_query += f" AND t.group_id = {group_id} group by \"year\""
        elif type_id:
            select_query += f" AND t.type_id = {type_id} group by \"year\""
        else:
            select_query += " group by \"year\""

        cursor_postgres.execute(select_query)
        rows = cursor_postgres.fetchall()
        # Extracting years and distances into separate lists
        years = [entry[0] for entry in rows]
        distances = [entry[1] for entry in rows]
        return [years, distances]



# ----------------------------------------------------------

# def get_speed(thing_id: Optional[int]=None, years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:


@app.get('/qa')
async def get_journies(page: int = 1, thing_id: Optional[int]=None, group_id: Optional[int]=None,type_id: Optional[int]=None ,years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
        get the last 5 journeies
    """ 


    # Calculate the offset
    offset = (page - 1) * 5

    select_query = f"""
            SELECT
                t.thing_id,
                active_time,
                avg_speed,
                max_speed,
                start_point,
                end_point,
                start_time,
                end_time,
                jd.path
            FROM
                journey j,
                journey_dim jd,
                thing_dim t,
                date_dim d
                WHERE
                jd.journey_id = j.journey_id
                AND j.thing_id = t.thing_id
                AND j.date_id = d.date_id
        """
    

    if thing_id :
        select_query += f""" AND j.thing_id = {thing_id} group by \"year\" ,	t.thing_id,
                    active_time,
                    avg_speed,
                    max_speed,
                    start_point,
                    end_point,
                    start_time,
                    end_time,
                    jd.path LIMIT 5 OFFSET {offset}"""

    elif group_id:
        select_query += f""" AND t.group_id = {group_id} group by \"year\"  ,	t.thing_id,
                    active_time,
                    avg_speed,
                    max_speed,
                    start_point,
                    end_point,
                    start_time,
                    end_time,
                    jd.path   LIMIT 5 OFFSET {offset}"""
    elif type_id:
        select_query += f""" AND t.type_id = {type_id} group by \"year\" ,	t.thing_id,
                    active_time,
                    avg_speed,
                    max_speed,
                    start_point,
                    end_point,
                    start_time,
                    end_time,
                    jd.path  LIMIT 5 OFFSET {offset}"""
    else:
        select_query += f""" group by \"year\"   ,	t.thing_id,
                    active_time,
                    avg_speed,
                    max_speed,
                    start_point,
                    end_point,
                    start_time,
                    end_time,
                    jd.path LIMIT 5 OFFSET {offset}"""

    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()
    # make a dict from that list

    result = []
    for row in rows:
        result.append({
            "thing_id": row[0],
            "active_time": row[1],
            "avg_speed": row[2],
            "max_speed": row[3],
            "start_point": row[4],
            "end_point": row[5],
            "start_time": row[6],
            "end_time": row[7],
            "path": row[8]
        })

    # return [11]
    return result





# UPDATE vehicle_performance
# SET avg_speed = 56, max_speed = 89
# WHERE thing_id = ' 629';

# INSERT INTO vehicle_performance (thing_id, active_time, idle_time) VALUES ('338', 10,11);
    

# get journies count
@app.get('/journycount')
async def get_journies_count(thing_id: Optional[int]=None, group_id: Optional[int]=None,type_id: Optional[int]=None ,years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    """
    

    select_query = f"""
            SELECT
                COUNT(j.journey_id)
            FROM
                journey j,
                journey_dim jd,
                thing_dim t,
                date_dim d
                WHERE
                jd.journey_id = j.journey_id
                AND j.thing_id = t.thing_id
                AND j.date_id = d.date_id
        """
    

    if thing_id :
        select_query += f""" AND j.thing_id = {thing_id} group by \"year\" """

    elif group_id:
        select_query += f""" AND t.group_id = {group_id} group by \"year\" """
    elif type_id:
        select_query += f""" AND t.type_id = {type_id} group by \"year\" """
    else:
        select_query += """ group by \"year\" """

    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    # make a dict from that list

    result = []
    for row in rows:
        result.append({
            "journey_count": row[0]
        })
    return result



# get number of vehicles
@app.get('/thingcount')
async def get_thing_count(group_id: Optional[int]=None,type_id: Optional[int]=None ,years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    """

    select_query = f"""
            SELECT
                COUNT(t.thing_id)
            FROM
                thing_dim t
        """
    

    if group_id:
        select_query += f""" WHERE t.group_id = {group_id} """

    elif type_id:
        select_query += f""" WHERE t.type_id = {type_id} """

    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    # make a dict from that list

    result = []
    for row in rows:
        result.append({
            "thing_count": row[0]
        })
    return result

# get number of active vehicles
@app.get('/activecount')
async def get_active_count(group_id: Optional[int]=None,type_id: Optional[int]=None ,years: Optional[str] = None, months: Optional[str] = None, d1: Optional[str] = None, d2: Optional[str] = None) -> Union[list, None]:
    """
    """

    select_query = f"""
            SELECT
                COUNT(t.thing_id)
            FROM
                thing_dim t,
                vehicle_peroformance v
            WHERE
                t.thing_id = v.thing_id
        """
    

    if group_id:
        select_query += f""" AND t.group_id = {group_id} """

    elif type_id:
        select_query += f""" AND t.type_id = {type_id} """

    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    # make a dict from that list

    result = []
    for row in rows:
        result.append({
            "active_count": 321
        })
    return result
    



# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# predictive maintance routes

# define a rout to get the historcal oil value
@app.get('/historical/oil')
async def get_historical_oil() -> Union[list, None]:
    """
    """


        # Connect to Cassandra
    cluster = Cluster(['localhost'])
    session = cluster.connect()


    # Use the keyspace
    session.set_keyspace('pfe')
    session.row_factory = dict_factory



    select_query = f"""
                select * from trace where thing_id = '1599' ORDER BY trace_date DESC limit 100 ;      
        """
    

   
    rows = session.execute(select_query)

    rows = list(rows)  # Convert rows to a list


    # Create two lists: one for the distances and one for the dates
    oil = [row['oil_value'] for row in rows]
    date = [row['trace_date'] for row in rows]


    date = [datetime.datetime.strptime(row['trace_date'], '%Y-%m-%d %H:%M:%S.%f').strftime('%M:%S') for row in rows]    # Close connection
    session.shutdown()

    # make a dict from that list
    return [oil,date]

 

# define a rout to get the historcal fuel values
@app.get('/historical/fuel')
async def get_historical_fuel() -> Union[list, None]:
    """
    """


        # Connect to Cassandra
    cluster = Cluster(['localhost'])
    session = cluster.connect()


    # Use the keyspace
    session.set_keyspace('pfe')
    session.row_factory = dict_factory



    select_query = f"""
                select * from trace where thing_id = '1599' ORDER BY trace_date DESC limit 100 ;      
        """
    

   
    rows = session.execute(select_query)

    rows = list(rows)
    
    # Create two lists: one for the distances and one for the dates
    fuel = [row['fuel_percent'] for row in rows]
    date = [row['trace_date'] for row in rows]

    date = [datetime.datetime.strptime(row['trace_date'], '%Y-%m-%d %H:%M:%S.%f').strftime('%M:%S') for row in rows]    # Close connection
    session.shutdown()

    # make a dict from that list
    return [fuel,date]



# define a route to get the historical battery volatge
@app.get('/historical/battery')
async def get_historical_battery() -> Union[list, None]:
    """
    """


        # Connect to Cassandra
    cluster = Cluster(['localhost'])
    session = cluster.connect()


    # Use the keyspace
    session.set_keyspace('pfe')
    session.row_factory = dict_factory



    select_query = f"""
                select * from trace where thing_id = '1599' ORDER BY trace_date DESC limit 100 ;      
        """
    

   
    rows = session.execute(select_query)

    rows = list(rows)
    
    # Create two lists: one for the distances and one for the dates
    battery = [row['battery_voltage'] for row in rows]
    date = [row['trace_date'] for row in rows]

    date = [datetime.datetime.strptime(row['trace_date'], '%Y-%m-%d %H:%M:%S.%f').strftime('%M:%S') for row in rows]    # Close connection
    session.shutdown()

    # make a dict from that list
    return [battery,date]





# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# #######################################
# LOGIN







# Secret key to encode the JWT token
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Fake database
fake_users_db = {
        "username": "p",
        "email": "p",
        "password": "p",  
        "disabled": False,
}

class LoginForm(BaseModel):
    username: str
    password: str

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    password2 = fake_users_db["password"] 
    user = fake_users_db.get(username)




    if  password2 != password:

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            # return tge user name and password
            detail="Invalid username or password " ,
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {"message": "Login successful"}

