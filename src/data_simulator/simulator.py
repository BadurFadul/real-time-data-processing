import json
import time
import random
import uuid
import boto3
import argparse
from datetime import datetime, timedelta
import threading
import signal
import sys

# Global flag for controlling the simulation
running = True

def generate_sensor_data(sensor_id):
    """Generate random sensor data"""
    return {
        'sensor_id': sensor_id,
        'temperature': round(random.uniform(-5, 40), 2),
        'humidity': round(random.uniform(0, 100), 2),
        'pressure': round(random.uniform(900, 1100), 2),
        'timestamp': datetime.utcnow().isoformat(),
        'battery': round(random.uniform(0, 100), 2)
    }

def send_to_kinesis(kinesis_client, stream_name, data, partition_key):
    """Send data to Kinesis stream"""
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=partition_key
    )
    return response

def simulate_sensors(stream_name, num_sensors=5, interval=0.2, duration=300):
    """
    Simulate multiple sensors sending data to Kinesis
    
    Args:
        stream_name: Name of the Kinesis stream
        num_sensors: Number of sensors to simulate
        interval: Time between data points (seconds)
        duration: Total duration of simulation (seconds)
    """
    global running
    
    # Initialize Kinesis client
    kinesis_client = boto3.client('kinesis')
    
    # Generate sensor IDs
    sensor_ids = [f"sensor-{i+1}" for i in range(num_sensors)]
    
    # Track metrics
    start_time = time.time()
    total_records = 0
    
    try:
        print(f"Starting simulation with {num_sensors} sensors for {duration} seconds")
        print(f"Sending data to Kinesis stream: {stream_name}")
        
        # Run until duration is reached or interrupted
        while running and (time.time() - start_time) < duration:
            # Generate and send data for each sensor
            for sensor_id in sensor_ids:
                # Generate random sensor data
                data = generate_sensor_data(sensor_id)
                
                # Send to Kinesis
                send_to_kinesis(kinesis_client, stream_name, data, sensor_id)
                total_records += 1
                
            # Print status update every 50 records
            if total_records % 50 == 0:
                elapsed = time.time() - start_time
                print(f"Sent {total_records} records in {elapsed:.2f} seconds " +
                      f"({total_records/elapsed:.2f} records/second)")
            
            # Wait for the next interval
            time.sleep(interval)
    
    except Exception as e:
        print(f"Error in simulation: {str(e)}")
    finally:
        elapsed_time = time.time() - start_time
        print(f"\nSimulation complete. Sent {total_records} records in {elapsed_time:.2f} seconds")
        print(f"Average throughput: {total_records/elapsed_time:.2f} records/second")

def signal_handler(sig, frame):
    """Handle interrupt signals to gracefully stop the simulation"""
    global running
    print("\nStopping simulation...")
    running = False

def main():
    parser = argparse.ArgumentParser(description='Sensor data simulator for Kinesis')
    parser.add_argument('--stream', required=True, help='Kinesis stream name')
    parser.add_argument('--sensors', type=int, default=5, help='Number of sensors to simulate')
    parser.add_argument('--interval', type=float, default=0.2, help='Interval between data points (seconds)')
    parser.add_argument('--duration', type=int, default=300, help='Duration of simulation (seconds)')
    
    args = parser.parse_args()
    
    # Set up signal handler for graceful termination
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start simulation
    simulate_sensors(args.stream, args.sensors, args.interval, args.duration)

if __name__ == "__main__":
    main() 