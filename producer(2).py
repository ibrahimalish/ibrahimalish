from time import sleep
import json
from kafka import KafkaProducer

# Define Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Path to your preprocessed JSON file
preprocessed_json_file = "preprocessed_data.json"

# Read preprocessed data from JSON file
with open(preprocessed_json_file, 'r') as file:
    preprocessed_data = json.load(file)

# Define the topic to which data will be sent
topic = 'preprocessed_data'

# Produce data to Kafka topic
for data_point in preprocessed_data:
    # Send data to Kafka topic
    producer.send(topic, value=data_point)
    # Print the sent data for verification
    print("Sent:", data_point)
    # Wait for a short interval before sending the next data
    sleep(1)

# Flush and close the producer
producer.flush()
producer.close()

