import json
from kafka import KafkaProducer
from time import sleep

# Kafka configuration
bootstrap_servers = ['localhost:9092']
topic = 'data3'

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Function to read data from JSON file line by line and stream it to Kafka
def stream_data_from_json_file(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            record = json.loads(line.strip())  # Load each line as a JSON object
            producer.send(topic, value=record)
            print('Record sent to Kafka:', record)

# Path to your preprocessed JSON file
json_file_path = '/home/ahmad/kafka/ass3/preprp/processed_data.json'

# Call the function to stream data from JSON file to Kafka
stream_data_from_json_file(json_file_path)

