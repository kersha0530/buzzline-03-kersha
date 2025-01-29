import os
import json
from collections import defaultdict
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load Environment Variables
load_dotenv()

# Getter Functions for .env Variables
def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "smoker_topic")  # Updated to use smoker topic
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "smoker_consumer_group")  # Updated to match consumer group
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

# Set up Data Store to hold sensor statuses
sensor_status_counts = defaultdict(int)

# Function to process a single message
def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            sensor_status = message_dict.get("sensor_status", "unknown")
            logger.info(f"Message received with sensor status: {sensor_status}")

            # Increment the count for the sensor status
            sensor_status_counts[sensor_status] += 1

            logger.info(f"Updated sensor status counts: {dict(sensor_status_counts)}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# Main Function for the consumer
def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # Fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")

    try:
        while True:  # Continuous polling
            # Polling Kafka for messages with a timeout
            messages = consumer.poll(timeout_ms=100)  # Adjust the timeout based on your needs
            for message in messages.get(topic, []):  # Ensure we are processing messages from the correct topic
                message_str = message.value.decode('utf-8')  # Decode bytes to string
                logger.debug(f"Received message at offset {message.offset}: {message_str}")
                process_message(message_str)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

# Conditional Execution
if __name__ == "__main__":
    main()




