# this code must run with python 3.8

import datetime
from datetime import datetime
from time import sleep
from kafka import KafkaProducer
import random

def produce_events(bootstrap_servers, topic):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # porduce a dict with trace_id,thing_id,trace_date,long,lat , speed and engine_status

            # 'longitude': random.uniform(3.354008, 3.454008),
            # 'latitude': random.uniform(36.605685, 36.695685),
    while True:
        event = {
            'thing_id': 627,
            'thing_name': 'car1',
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(1.0, 2.3),
            'latitude': random.uniform(27.0, 29.0),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['1', '0']),
            'oil_value': 2,
            'fuel_liters': random.randint(1, 10),
            'fuel_percent': random.randint(1, 100),
            "battery": random.randint(10,16)
            
        }

        event2 = {
            'thing_id': 337,
            'thing_name': 'car2',
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(1.0, 2.3),
            'latitude': random.uniform(27.0, 29.0),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['1', '0']),
            'oil_value': random.randint(2, 5),
            'fuel_liters': random.randint(1, 10),
            'fuel_percent': random.randint(1, 100),
            "battery": random.randint(10,16)

        }

        event3 = {
            'thing_id': 629,
            'thing_name': 'car3',
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(1.0, 2.3),
            'latitude': random.uniform(27.0, 29.0),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['1', '0']),
            'oil_value': random.randint(2, 5),
            'fuel_liters': random.randint(1, 10),
            'fuel_percent': random.randint(1, 100),
            "battery": random.randint(10,16)
        }

        event4 = {
            'thing_id': 1599,
            'thing_name': 'car4',
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(1.0, 2.3),
            'latitude': random.uniform(27.0, 29.0),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['1', '0']),
            'oil_value': random.randint(2, 5),
            'fuel_liters': random.randint(1, 10),
            'fuel_percent': random.randint(1, 100),
            "battery": random.randint(10,16)
        }


        event5 = {
            'thing_id': 338,
            'thing_name': 'car5',
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(1.0, 2.3),
            'latitude': random.uniform(27.0, 29.0),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['1', '0']),
            'oil_value': random.randint(2, 5),
            'fuel_liters': random.randint(1, 10),
            'fuel_percent': random.randint(1, 100),
            "battery": random.randint(10,16)
        }

        event6 = {
            'thing_id': 339,
            'thing_name': 'car6',
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(1.0, 2.3),
            'latitude': random.uniform(27.0, 29.0),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['1', '0']),
            'oil_value': random.randint(2, 5),
            'fuel_liters': random.randint(1, 10),
            'fuel_percent': random.randint(1, 100),
            "battery": random.randint(10,16)
        }





        producer.send(topic, value=str(event).encode('utf-8'))
        producer.send(topic, value=str(event2).encode('utf-8'))
        producer.send(topic, value=str(event3).encode('utf-8'))
        producer.send(topic, value=str(event4).encode('utf-8'))
        # producer.send(topic, value=str(event5).encode('utf-8'))
        # producer.send(topic, value=str(event6).encode('utf-8'))





        producer.flush()

        sleep(10)

        print('Events produced successfully!')
        print(event4)
if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'  
    topic = 'iotevents'  

    produce_events(bootstrap_servers, topic)