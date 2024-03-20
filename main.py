
from typing import Union
from cassandra.cluster import Cluster
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2

app = FastAPI()

# Establish a connection to the PostgreSQL server
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="geopfe",
    user="postgres",
    password="ryqn"
)

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


@app.get("/get_data/{thing_id}")
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
        v.thing_id,
        t.thing_name,
        v.date_id,
        d.month_name,
        idle_time,
        active_time
    FROM
        vehicle_peroformance v,
        thing_dim t,
        date_dim d
    WHERE
        v.date_id = d.date_id
        AND v.thing_id = t.thing_id
        AND v.thing_id = {thing_id}
    GROUP BY
        t.thing_name,
        v.thing_id,
        v.date_id,
        idle_time,
        d.month_name,
        active_time;
    """
    cursor_postgres.execute(select_query)
    rows = cursor_postgres.fetchall()

    return rows
