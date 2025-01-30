import os
import sys
import time
import pathlib
import csv
import json 
from datetime import datetime
from dotenv import load_dotenv
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger


# Load Environment Variables
load_dotenv()

# Getter Functions for .env Variables
def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "smoker_topic")  # Updated topic for smoker data
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))  # Default to 1 second
    logger.info(f"Message interval: {interval} seconds")
    return interval

# Set up Paths
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("smoker_data_random.csv")  # Updated to reference the new random data CSV file
logger.info(f"Data file: {DATA_FILE}")

# Message Generator
def generate_messages(file_path: pathlib.Path):
    """
    Read from a CSV file and yield them one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the CSV file.

    Yields:
        dict: A dictionary containing the CSV data with custom fields.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r") as csv_file:
                logger.info(f"Reading data from file: {file_path}")

                # Read the CSV file as a dictionary
                csv_reader = csv.DictReader(csv_file)

                # Iterate over the rows in the CSV file
                for entry in csv_reader:
                    # Adding custom fields
                    sensor_status = entry.get("sensor_status", "inactive")
                    user_temp_setting = entry.get("user_temp_setting", "N/A")
                    remote_control_status = entry.get("remote_control_status", "N/A")
                    temperature = float(entry.get("temperature", 0))

                    # Prepare the message structure with the custom fields
                    message = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "sensor_status": sensor_status,
                        "user_temp_setting": user_temp_setting,
                        "remote_control_status": remote_control_status,
                    }

                    # Added custom logic for temperature-related messages
                    if sensor_status == "active":
                        message["status_message"] = "Sensor is active."
                    else:
                        message["status_message"] = "Sensor is inactive."

                    # Logic to add temperature status messages
                    if temperature > 80:
                        message["temperature_status"] = "Warning: It's too hot!"
                    elif temperature < 40:
                        message["temperature_status"] = "Warning: It's too cold!"
                    else:
                        message["temperature_status"] = "Temperature is within range."

                    # Log the generated message for debugging
                    logger.debug(f"Generated message: {message}")
                    yield message

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

# Main Function
def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated messages to the Kafka topic.
    """
    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message_dict in generate_messages(DATA_FILE):
            # Send message directly as a dictionary (producer handles serialization)
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")

# Conditional Execution
if __name__ == "__main__":
    main()

