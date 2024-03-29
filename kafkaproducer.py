# this code must run with python 3.8

import datetime
from datetime import datetime
from time import sleep
from kafka import KafkaProducer
import random

def produce_events(bootstrap_servers, topic):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # porduce a dict with trace_id,thing_id,trace_date,long,lat , speed and engine_status


    while True:
        event = {
            'thing_id': 627,
            'trace_date': str(datetime.now()),
            'long': random.uniform(-180, 180),
            'lat': random.uniform(-90, 90),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['running', 'stopped'])
        }

        event2 = {
            'thing_id': 337,
            'trace_date': str(datetime.now()),
            'long': random.uniform(-180, 180),
            'lat': random.uniform(-90, 90),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['running', 'stopped'])
        }

        producer.send(topic, value=str(event).encode('utf-8'))
        producer.send(topic, value=str(event2).encode('utf-8'))



        producer.flush()

        sleep(1)
if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'  
    topic = 'iotevents'  

    produce_events(bootstrap_servers, topic)