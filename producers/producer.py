from kafka import KafkaProducer

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Send messages to the 'test-topic'
for i in range(5):
    message = f"Message {i}"
    producer.send('test-topic', value=message.encode('utf-8'))
    print(f"Sent: {message}")

# Close the producer
producer.close()
