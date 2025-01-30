import pandas as pd
import random
from datetime import datetime, timedelta

# Define your file paths
output_custom_file = "smoker_data_random.csv"
original_file = "smoker_temps.csv"
output_combined_file = "smoker_temps_combined.csv"

# Generate random data for 241 entries
start_time = datetime(2025, 1, 1, 15, 0, 0)  # Starting from 3:00 PM
timestamps = [start_time + timedelta(minutes=i) for i in range(241)]
temperatures = [round(random.uniform(150.0, 170.0), 2) for _ in range(241)]  # Random temperature between 150F and 170F
sensor_status = ["inactive" if i % 2 == 0 else "active" for i in range(241)]  # Alternating sensor status
user_temp_setting = [random.choice([150, 160, 170]) for _ in range(241)]  # Random temp settings
remote_control_status = ["OFF" if i % 2 == 0 else "ON" for i in range(241)]  # Alternating control status
sensor_activity = ["active" if i % 2 == 0 else "inactive" for i in range(241)]  # Alternating activity status
status_message = ["Sensor is inactive." if s == "inactive" else "Sensor is active." for s in sensor_status]
temperature_status = ["Warning: It's too cold!" if temp < 160 else "Temperature is within range." for temp in temperatures]

# Create a DataFrame for the random data
custom_data = pd.DataFrame({
    "timestamp": timestamps,
    "temperature": temperatures,
    "sensor_status": sensor_status,
    "user_temp_setting": user_temp_setting,
    "remote_control_status": remote_control_status,
    "sensor_activity": sensor_activity,
    "status_message": status_message,
    "temperature_status": temperature_status
})

# Save the custom random data to a CSV file
custom_data.to_csv(output_custom_file, index=False)
print(f"New custom CSV file generated: {output_custom_file}")
