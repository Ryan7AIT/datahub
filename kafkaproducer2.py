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
        alert1 = {
            'type_id': 1,
            'user_id': 1,
            'event_date': str(datetime.now()),
            'latitude': random.uniform(1.0, 2.3),
            'longitude': random.uniform(27.0, 29.0),
            'event_id': '1',
            'type_id': 1,
            
        } 

        alert2 = {
            'type_id': 2,
            'user_id': 2,
            'event_date': str(datetime.now()),
            'latitude': random.uniform(1.0, 2.3),
            'longitude': random.uniform(27.0, 29.0),
            'event_id': '2',
            'type_id': 2,
            
        }

        producer.send(topic, value=str(alert1).encode('utf-8'))
        producer.send(topic, value=str(alert2).encode('utf-8'))






        producer.flush()

        sleep(2)

        print('Events produced successfully!')
        print(alert2)

        
if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'  
    topic = 'iotalerts'  

    produce_events(bootstrap_servers, topic)