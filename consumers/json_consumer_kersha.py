import os
import json
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load Environment Variables
load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("BUZZ_TOPIC", "buzztopic")  # Same topic as the producer
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("BUZZ_CONSUMER_GROUP_ID", "buzz_consumer_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        message_dict = json.loads(message)
        logger.info(f"Received message: {message_dict}")
        
        # Extract fields
        message_text = message_dict.get("message", "")
        author = message_dict.get("author", "")
        
        logger.info(f"Message from {author}: {message_text}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main() -> None:
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")

    try:
        while True:
            messages = consumer.poll(timeout_ms=100)
            for message in messages.get(topic, []):
                message_str = message.value.decode('utf-8')
                process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

if __name__ == "__main__":
    main()





