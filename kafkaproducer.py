import random
import pandas as pd
from datetime import datetime
from time import sleep
from kafka import KafkaProducer

def produce_events_from_multiple_csv(bootstrap_servers, topic, csv_file_paths):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    
    # Read the CSV files into a list of DataFrames
    dataframes = [pd.read_csv(path) for path in csv_file_paths]

    # Initialize values
    initial_fuel_liters = 57
    initial_fuel_percent = 91
    fuel_liters_decrement = random.uniform(0.017,0.025)
    fuel_percent_decrement = 0.011

    # Set initial values for fuel_liters and fuel_percent
    current_fuel_liters = initial_fuel_liters
    current_fuel_percent = initial_fuel_percent


    
    # Make sure each DataFrame has the same structure
    for df in dataframes:
        if 'thing_id' not in df.columns or 'longitude' not in df.columns or 'latitude' not in df.columns or 'speed' not in df.columns or 'engine_status' not in df.columns or 'battery_current' not in df.columns:
            raise ValueError(f"CSV file {df} does not have the required columns.")
    
    index = 0
    while True:
        for df in dataframes:
            if index < len(df):

                current_fuel_liters = max(0, current_fuel_liters - fuel_liters_decrement)
                current_fuel_percent = max(0, current_fuel_percent - fuel_percent_decrement)


                row = df.iloc[index]
                event = {
                    'thing_id': row['thing_id'],
                    'thing_name': f'car{row["thing_id"]}',
                    'trace_date': str(datetime.now()),
                    'longitude': row['longitude'],
                    'latitude': row['latitude'],
                    'speed': row['speed'],
                    'engine_status': row['engine_status'],
                    'oil_value': 4,
                    'fuel_liters': current_fuel_liters,
                    'fuel_percent': current_fuel_percent,
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


    ]  

    produce_events_from_multiple_csv(bootstrap_servers, topic, csv_file_paths)
