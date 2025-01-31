# Project: Buzzline-03-Kersha

## Overview
This repository contains scripts and resources for simulating and consuming data for a "smart smoker" application. The data is produced using Kafka and consumed from Kafka topics into CSV files. This project involves generating random smoker data and modifying consumer scripts to handle and store the data efficiently.

### File Descriptions
1. **smoker_temps.csv**  
   **Purpose:** This CSV file contains the base temperature data of the smoker, with timestamps and temperature readings at regular intervals.  
   **Details:**  
   - The file includes columns for timestamp and temperature.  
   - The data is generated as part of the project to simulate smoker temperature readings over time.

2. **smoker_data_random.csv**  
   **Purpose:** This CSV file contains randomized data for testing purposes, simulating various scenarios of a smoker’s operation.  
   **Details:**  
   - Columns: timestamp, temperature, sensor_status, user_temp_setting, remote_control_status, sensor_activity, status_message, temperature_status.  
   **Customizations:**  
   - `sensor_status`: Alternates between active and inactive.  
   - `user_temp_setting`: Randomized between 150, 160, and 170°F.  
   - `remote_control_status`: Alternates between ON and OFF.  
   - `sensor_activity`: Alternates between active and inactive.  
   - `status_message`: Based on the `sensor_status`, it shows whether the sensor is active or inactive.  
   - `temperature_status`: If the temperature is above 80°F, it shows "Warning: It's too hot!" If below 40°F, "Warning: It's too cold!" Otherwise, it indicates the temperature is within range.

**Producer Script (csv_producer_kersha.py)**
**Purpose:** This Python script generates messages based on the data in smoker_data_random.csv and sends them to a Kafka topic.
**Details:**
Uses the smoker_data_random.csv as input to generate and send messages with the sensor_status, temperature, and related custom fields to the Kafka topic.
The script runs continuously, producing messages every 5 seconds (can be configured in the .env).
**Consumer Script (csv_consumer_kersha.py)**
**Purpose:** This Python script consumes the messages from the Kafka topic and writes them to a CSV file.
**Details:**
Reads messages from the Kafka topic (smoker_topic) and writes the data into output.csv.
The consumer logs the process and writes additional information (like status_message, temperature_status, etc.) to the output file.
### Customizations Added
1. Randomized Data Generation
For testing, a randomized dataset (smoker_data_random.csv) has been added. The dataset includes variables such as sensor_status, temperature, user_temp_setting, and more, to simulate the behavior of a smart smoker in various conditions.
2. **Producer and Consumer**
**Producer:**
Sends messages at intervals to Kafka, ensuring the simulation data is streamed to the smoker_topic.
Added logic to handle the status of the sensor and related temperature thresholds.
**Consumer:**
Reads from the Kafka topic and writes the data to a CSV file (output.csv).
The script processes the received messages, converting them into a structured CSV file that can be easily used for further analysis.
3. **Kafka Integration**
The producer script connects to the Kafka broker, publishes data, and manages message flow.
The consumer script connects to the same Kafka broker and consumes the data, converting the received messages into a CSV format.
 
**buzz.json**  
**Purpose:** A supporting JSON file that contains family-friendly messages, serving as an input resource for the producer and consumer scripts.  
**Details:**  
   - The file is used by the producer to stream messages and by the consumer to receive and process the messages.  
   - **Example data:**  
     ```json
     [
       {
         "message": "The best part of the day is family time! 🌟",
        "author": "Kersha"
       },
       {
         "message": "Let's cook something tasty today! 🍳",
         "author": "Carlos"
       },
       {
         "message": "Don't forget to smile, it's a beautiful day! 😊",
         "author": "Ian"
       },
       {
         "message": "Time for some fun and games with the family! 🧩",
         "author": "Kairo"
       }
     ]
     ```

4. **Producer Script (json_producer_kersha.py)**  
   **Purpose:** This Python script generates messages based on the data in `buzz.json` and sends them to a Kafka topic.  
   **Details:**  
   - Uses the `buzz.json` as input to generate and send messages with the family-friendly messages and related custom fields to the Kafka topic.  
   - The script runs continuously, producing messages at intervals (default: 1 second).

   **Running the Producer**
To start the producer and begin sending messages to Kafka, run the following:

```bash```

python producers/json_producer_kersha.py


***Environment Configuration: .env***
Ensure both scripts use the same Kafka topic (smoker_topic):
```env```

SMOKER_TOPIC=smoker_topic
BUZZ_TOPIC=smoker_topic
SMOKER_CONSUMER_GROUP_ID=smoker_consumer_group


5. **Consumer Script (json_consumer_kersha.py)**  
   **Purpose:** This Python script consumes the messages from the Kafka topic and prints or processes the data.  
   **Details:**  
   - Reads messages from the Kafka topic (`buzztopic`) and processes them.  
   - The consumer logs and prints the message content (author and message).


 **Running the Consumer**
To start the consumer and consume messages from Kafka:

```bash```

python consumers/json_consumer_kersha.py


### Customizations Added
1. **Randomized Data Generation**  
   For testing, a randomized dataset (`smoker_data_random.csv`) has been added. The dataset includes variables such as `sensor_status`, `temperature`, `user_temp_setting`, and more, to simulate the behavior of a smart smoker in various conditions.

2. **Producer and Consumer**  
   **Producer:**  
   - Sends messages at intervals to Kafka, ensuring the simulation data is streamed to the `buzztopic`.  
   - Added logic to handle the status of the sensor and related temperature thresholds.  
   **Consumer:**  
   - Reads from the Kafka topic and processes the data.
   - Converts the received messages into a CSV format or prints to the console.

3. **Kafka Integration**  
   The producer script connects to the Kafka broker, publishes data, and manages message flow.  
   The consumer script connects to the same Kafka broker and consumes the data, processing the received messages.


### Setup and Usage
1. Dependencies
Install required Python packages:

```bash```

pip install -r requirements.txt
2. .env Configuration
Create a .env file in the project root directory to configure the Kafka settings:

```bash```

KAFKA_BROKER=localhost:9092
BUZZ_TOPIC=buzztopic
BUZZ_INTERVAL_SECONDS=5
CSV_OUTPUT_FILE=output.csv
BUZZ_CONSUMER_GROUP_ID=buzz_consumer_group

3. Running the Producer
To start the producer and begin sending messages to Kafka, run the following:

```bash```

python producers/csv_producer_kersha.py
4. Running the Consumer
To start the consumer and consume messages from Kafka:

```bash```

python consumers/csv_consumer_kersha.py
### Notes
Ensure your Kafka and Zookeeper are running before starting the producer and consumer.
Adjust the .env file for any custom configurations (e.g., Kafka broker address, topic name).


## About the Smart Smoker (CSV Example)

A food stall occurs when the internal temperature of food plateaus or 
stops rising during slow cooking, typically between 150°F and 170°F. 
This happens due to evaporative cooling as moisture escapes from the 
surface of the food. The plateau can last for hours, requiring 
adjustments like wrapping the food or raising the cooking temperature to 
overcome it. Cooking should continue until the food reaches the 
appropriate internal temperature for safe and proper doneness.

The producer simulates a smart food thermometer, sending a temperature 
reading every 15 seconds. The consumer monitors these messages and 
maintains a time window of the last 5 readings. 
If the temperature varies by less than 2 degrees, the consumer alerts 
the BBQ master that a stall has been detected. This time window helps 
capture recent trends while filtering out minor fluctuations.

## Later Work Sessions
When resuming work on this project:
1. Open the folder in VS Code. 
2. Start the Zookeeper service.
3. Start the Kafka service.
4. Activate your local project virtual environment (.env).

## Save Space
To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later. 
Managing Python virtual environments is a valuable skill. 

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.
