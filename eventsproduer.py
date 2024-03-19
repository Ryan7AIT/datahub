import time
import time
from kafka import KafkaProducer

def send_event_to_kafka(producer, topic, event):
    producer.send(topic, event.encode('utf-8'))
    producer.flush()

def generate_event():
    # Replace this with your own logic to generate the event
    return "Event generated at {}".format(time.time())

def main():
    # Configure Kafka producer
    kafka_bootstrap_servers = 'localhost:9092'  # Replace with your Kafka bootstrap servers
    kafka_topic = 't'  # Replace with your Kafka topic

    producer_config = {
        'bootstrap_servers': kafka_bootstrap_servers
    }

    producer = KafkaProducer(**producer_config)

    while True:
        event = generate_event()
        send_event_to_kafka(producer, kafka_topic, event)
        print(event)
        print('Event sent to Kafka')
        time.sleep(1)

if __name__ == '__main__':
    main()