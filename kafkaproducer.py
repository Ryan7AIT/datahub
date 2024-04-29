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
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(1.0, 2.3),
            'latitude': random.uniform(27.0, 29.0),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['1', '0']),
            'oil_value': random.randint(2, 5),
            'fuel_liters': random.randint(1, 10),
            'fuel_percent': random.randint(1, 100),
            "battery": random.randint(10,16)


            # {"oil_value": "0", "fuel_liters": "0", "frigo_status": "0", "fuel_percent": "0", "liquid_value": "0", "is_new_changes":
            #   1, "is_new_position": 1, "frigo_temperature": "0", "max_acceleration_value": null, "max_deceleration_value": null}

            
        }

        event2 = {
            'thing_id': 337,
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(19.06, 12.00),
            'latitude': random.uniform(37.09, 15.00),
            'speed': random.randint(10, 100),
            'engine_status': random.choice(['1', '0']),
            'oil_value': random.randint(2, 5),
            'fuel_liters': random.randint(1, 10),
            'fuel_percent': random.randint(1, 100),
            "battery": random.randint(10,16)

        }

        event3 = {
            'thing_id': 629,
            'trace_date': str(datetime.now()),
            'longitude': random.uniform(19.06, 12.00),
            'latitude': random.uniform(37.09, 15.00),
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




        producer.flush()

        sleep(10)

        print('Events produced successfully!')
        print(event)
if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'  
    topic = 'iotevents'  

    produce_events(bootstrap_servers, topic)