import os
import csv
import json
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "smoker_topic")  # Ensure this topic matches your producer's topic
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id = os.getenv("KAFKA_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_output_csv_file() -> str:
    """Fetch the CSV output file name from the environment or use default."""
    file_name = os.getenv("CSV_OUTPUT_FILE", "output.csv")
    logger.info(f"CSV Output file: {file_name}")
    return file_name

#####################################
# Function to process a JSON message and convert to CSV
#####################################

def process_message(message: str, csv_writer: csv.writer, csv_file) -> None:
    """
    Process a JSON message and write the relevant fields to the CSV file.

    Args:
        message (str): JSON message received from Kafka.
        csv_writer (csv.writer): CSV writer object.
        csv_file (file): The CSV file to which the data is written.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Convert the message (JSON string) into a dictionary
        data = json.loads(message)

        # Extract relevant fields from the message
        timestamp = data.get("timestamp")
        temperature = data.get("temperature")
        sensor_status = data.get("sensor_status", "N/A")
        user_temp_setting = data.get("user_temp_setting", "N/A")
        remote_control_status = data.get("remote_control_status", "N/A")
        sensor_activity = data.get("sensor_activity", "N/A")
        status_message = data.get("status_message", "N/A")
        temperature_status = data.get("temperature_status", "N/A")

        logger.info(f"Processed message: {data}")

        # Write the data to the CSV file
        csv_writer.writerow([timestamp, temperature, sensor_status, user_temp_setting, remote_control_status, sensor_activity, status_message, temperature_status])
        logger.info(f"Written to CSV: {data}")

    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

#####################################
# Main Function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.
    
    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # Fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    output_file = get_output_csv_file()

    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"CSV output file: {output_file}")

    # Open the CSV file in write mode and create a CSV writer object
    with open(output_file, mode="a", newline="") as file:
        csv_writer = csv.writer(file)
        # Write CSV header if it's a new file
        file.seek(0, os.SEEK_END)  # Go to the end of the file
        if file.tell() == 0:  # Check if the file is empty
            csv_writer.writerow(['timestamp', 'temperature', 'sensor_status', 'user_temp_setting', 'remote_control_status', 'sensor_activity', 'status_message', 'temperature_status'])  # CSV header

        # Create the Kafka consumer
        consumer = create_kafka_consumer(topic, group_id)

        logger.info(f"Polling messages from topic '{topic}'...")

        try:
            for message in consumer:
                message_str = message.value.decode('utf-8')  # Ensure message is decoded from bytes
                logger.debug(f"Received message at offset {message.offset}: {message_str}")
                process_message(message_str, csv_writer, file)
        except KeyboardInterrupt:
            logger.warning("Consumer interrupted by user.")
        except Exception as e:
            logger.error(f"Error while consuming messages: {e}")
        finally:
            consumer.close()
            logger.info(f"Kafka consumer for topic '{topic}' closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()

