# # this code must run with python 3.8

# import datetime
# from datetime import datetime
# from time import sleep
# from kafka import KafkaProducer
import random

# def produce_events(bootstrap_servers, topic):
#     producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

#     # porduce a dict with trace_id,thing_id,trace_date,long,lat , speed and engine_status

#             # 'longitude': random.uniform(3.354008, 3.454008),
#             # 'latitude': random.uniform(36.605685, 36.695685),
#     while True:
#         event = {
#             'thing_id': 627,
#             'thing_name': 'car1',
#             'trace_date': str(datetime.now()),
#             'longitude': random.uniform(1.0, 2.3),
#             'latitude': random.uniform(27.0, 29.0),
#             'speed': random.randint(10, 100),
#             'engine_status': random.choice(['1', '0']),
#             'oil_value': 2,
#             'fuel_liters': random.randint(1, 10),
#             'fuel_percent': random.randint(1, 100),
#             "battery": random.randint(10,16)
            
#         }

#         event2 = {
#             'thing_id': 337,
#             'thing_name': 'car2',
#             'trace_date': str(datetime.now()),
#             'longitude': random.uniform(1.0, 2.3),
#             'latitude': random.uniform(27.0, 29.0),
#             'speed': random.randint(10, 100),
#             'engine_status': random.choice(['1', '0']),
#             'oil_value': random.randint(2, 5),
#             'fuel_liters': random.randint(1, 10),
#             'fuel_percent': random.randint(1, 100),
#             "battery": random.randint(10,16)

#         }

#         event3 = {
#             'thing_id': 629,
#             'thing_name': 'car3',
#             'trace_date': str(datetime.now()),
#             'longitude': random.uniform(1.0, 2.3),
#             'latitude': random.uniform(27.0, 29.0),
#             'speed': random.randint(10, 100),
#             'engine_status': random.choice(['1', '0']),
#             'oil_value': random.randint(2, 5),
#             'fuel_liters': random.randint(1, 10),
#             'fuel_percent': random.randint(1, 100),
#             "battery": random.randint(10,16)
#         }

#         event4 = {
#             'thing_id': 1599,
#             'thing_name': 'car4',
#             'trace_date': str(datetime.now()),
#             'longitude': random.uniform(1.0, 2.3),
#             'latitude': random.uniform(27.0, 29.0),
#             'speed': random.randint(10, 100),
#             'engine_status': random.choice(['1', '0']),
#             'oil_value': random.randint(2, 5),
#             'fuel_liters': random.randint(1, 10),
#             'fuel_percent': random.randint(1, 100),
#             "battery": random.randint(10,16)
#         }






#         # producer.send(topic, value=str(event).encode('utf-8'))
#         # producer.send(topic, value=str(event2).encode('utf-8'))
#         # producer.send(topic, value=str(event3).encode('utf-8'))
#         # producer.send(topic, value=str(event4).encode('utf-8'))
#         # producer.send(topic, value=str(event5).encode('utf-8'))
#         # producer.send(topic, value=str(event6).encode('utf-8'))





#         producer.flush()

#         sleep(10)

#         print('Events produced successfully!')
#         print(event4)
# if __name__ == '__main__':
#     bootstrap_servers = 'localhost:9092'  
#     topic = 'iotevents'  

#     produce_events(bootstrap_servers, topic)








import pandas as pd
from datetime import datetime
from time import sleep
from kafka import KafkaProducer

def produce_events_from_multiple_csv(bootstrap_servers, topic, csv_file_paths):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    
    # Read the CSV files into a list of DataFrames
    dataframes = [pd.read_csv(path) for path in csv_file_paths]
    
    # Make sure each DataFrame has the same structure
    for df in dataframes:
        if 'thing_id' not in df.columns or 'longitude' not in df.columns or 'latitude' not in df.columns or 'speed' not in df.columns or 'engine_status' not in df.columns or 'battery_current' not in df.columns:
            raise ValueError(f"CSV file {df} does not have the required columns.")
    
    index = 0
    while True:
        for df in dataframes:
            if index < len(df):
                row = df.iloc[index]
                event = {
                    'thing_id': row['thing_id'],
                    'thing_name': f'car{row["thing_id"]}',
                    'trace_date': str(datetime.now()),
                    'longitude': row['longitude'],
                    'latitude': row['latitude'],
                    'speed': row['speed'],
                    'engine_status': row['engine_status'],
                    'oil_value': random.randint(2, 5),
                    'fuel_liters': random.randint(10, 20),
                    'fuel_percent': random.randint(10, 40),
                    'battery': row['battery_current']
                }

                producer.send(topic, value=str(event).encode('utf-8'))
                print(f'Event produced successfully: {event}')
        sleep(10)

        
        producer.flush()
        
        print('All events in this round produced successfully!')

        index += 1
        if index >= max(len(df) for df in dataframes):
            index = 0
        

if __name__ == '__main__':
    bootstrap_servers = 'localhost:9092'  
    topic = 'iotevents'
    csv_file_paths = [
        '/Users/mac/Downloads/car629.csv', 
        '/Users/mac/Downloads/car1599.csv', 
        '/Users/mac/Downloads/car338.csv', 
        '/Users/mac/Downloads/car3152.csv',
        '/Users/mac/Downloads/car3038.csv',
        '/Users/mac/Downloads/car4775.csv',
        '/Users/mac/Downloads/car2239.csv',
        '/Users/mac/Downloads/car4699.csv',


    ]  # Update these paths to your CSV file paths

    produce_events_from_multiple_csv(bootstrap_servers, topic, csv_file_paths)
