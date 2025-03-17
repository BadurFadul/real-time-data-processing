import json
import os
import base64
import time
import boto3
import uuid
from datetime import datetime, timezone
from decimal import Decimal

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def process_sensor_data(sensor_data):
    """
    Process the sensor data by adding derived fields and transformations
    """
    # Create a copy of the input data
    processed_data = dict(sensor_data)
    
    # Convert all float values to Decimal for DynamoDB compatibility
    for key, value in processed_data.items():
        if isinstance(value, float):
            processed_data[key] = Decimal(str(value))
    
    # Add a unique ID if not present
    if 'id' not in processed_data:
        processed_data['id'] = processed_data.get('sensor_id', str(uuid.uuid4()))
    
    # Add processing timestamp
    processed_data['processed_at'] = datetime.now(timezone.utc).isoformat()
    
    # Filter negative temperature to 0
    if 'temperature' in processed_data and processed_data['temperature'] < 0:
        processed_data['temperature'] = Decimal('0')
    
    # Add temperature category based on value
    temp = processed_data.get('temperature', 0)
    if temp < 0:
        processed_data['category'] = 'freezing'
    elif temp < 18:
        processed_data['category'] = 'cold'
    elif temp < 24:
        processed_data['category'] = 'normal'  # Changed from 'comfortable' to match test expectations
    elif temp < 30:
        processed_data['category'] = 'warm'
    else:
        processed_data['category'] = 'hot'
    
    return processed_data

def store_processed_data(processed_data):
    """
    Store the processed data in DynamoDB
    """
    # Ensure we have a timestamp for the sort key
    if 'timestamp' not in processed_data:
        processed_data['timestamp'] = datetime.now(timezone.utc).isoformat()
    
    # Print the data being stored for debugging
    print(f"Storing data in DynamoDB: {json.dumps(processed_data, default=str)}")
    
    # Store in DynamoDB
    try:
        table.put_item(Item=processed_data)
        print(f"Successfully stored item with id {processed_data.get('id')} in DynamoDB")
        return processed_data
    except Exception as e:
        print(f"Error storing data in DynamoDB: {str(e)}")
        raise e

def lambda_handler(event, context):
    """
    Lambda function to process sensor data from Kinesis stream
    and store processed results in DynamoDB
    """
    start_time = time.time()
    processed_count = 0
    
    try:
        print(f"Received event: {json.dumps(event)}")
        
        for record in event['Records']:
            # Decode and parse the Kinesis record
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            print(f"Decoded payload: {payload}")
            sensor_data = json.loads(payload)
            
            # Process the data (filtering, transformation, aggregation)
            processed_data = process_sensor_data(sensor_data)
            
            # Store processed data in DynamoDB
            store_processed_data(processed_data)
            processed_count += 1
            
        processing_time = time.time() - start_time
        
        # Log performance metrics
        print(f"Successfully processed {processed_count} records in {processing_time:.3f} seconds")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed_records': processed_count,
                'processing_time_seconds': processing_time
            })
        }
        
    except Exception as e:
        print(f"Error processing records: {str(e)}")
        raise e 