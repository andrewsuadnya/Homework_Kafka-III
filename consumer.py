from confluent_kafka.avro import AvroConsumer

# Configure the AvroConsumer
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'bitcoin_price_group',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': 'http://localhost:8081'
}
consumer = AvroConsumer(consumer_config)
consumer.subscribe(['bitcoin_price_topic'])

# Consume messages from Kafka
while True:
    try:
        msg = consumer.poll(10)
        if msg is None:
            continue
        record_key = msg.key()
        record_value = msg.value()
        print(f"Key: {record_key}, Value: {record_value}")
    except Exception as e:
        print(f"Error: {e}")
        break

consumer.close()
