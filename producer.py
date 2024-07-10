from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import pandas as pd

# Load the Avro schemas
key_schema_path = 'bitcoin_key.avsc'
value_schema_path = 'bitcoin_value.avsc'

with open(key_schema_path, 'r') as key_file:
    key_schema_str = key_file.read()
key_schema = avro.loads(key_schema_str)

with open(value_schema_path, 'r') as value_file:
    value_schema_str = value_file.read()
value_schema = avro.loads(value_schema_str)

# Configure the AvroProducer
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
}
producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

# Load the Bitcoin data
data_path = 'data/bitcoin_price_Training.csv'
bitcoin_data = pd.read_csv(data_path)

# Produce messages to Kafka
topic = 'bitcoin_price_topic'
for index, row in bitcoin_data.iterrows():
    record_key = {"Date": row["Date"]}
    record_value = row.to_dict()
    producer.produce(topic=topic, key=record_key, value=record_value)
    producer.flush()

print("Data has been produced to Kafka topic.")
