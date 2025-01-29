from kafka import KafkaConsumer

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='test-group'
)

# Consume messages from the 'test-topic'
print("Starting Consumer...")
for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
