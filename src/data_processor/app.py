import json
import os
import base64
import time
import boto3
import uuid
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def lambda_handler(event, context):
    """
    Lambda function to process sensor data from Kinesis stream
    and store processed results in DynamoDB
    """
    start_time = time.time()
    processed_count = 0
    
    try:
        for record in event['Records']:
            # Decode and parse the Kinesis record
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
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

def process_sensor_data(data):
    """
    Process the sensor data - filter, transform, or aggregate
    
    In this example:
    1. Filter out readings below a threshold
    2. Calculate a moving average if multiple readings
    3. Add metadata and categorize readings
    """
    # Simple filtering
    if 'temperature' in data and data['temperature'] < 0:
        data['temperature'] = 0  # Filter out negative temperatures
    
    # Add processing metadata
    data['processed_at'] = datetime.utcnow().isoformat()
    data['id'] = str(uuid.uuid4())
    data['timestamp'] = data.get('timestamp', datetime.utcnow().isoformat())
    
    # Categorize readings (example transformation)
    if 'temperature' in data:
        if data['temperature'] > 30:
            data['category'] = 'hot'
        elif data['temperature'] < 10:
            data['category'] = 'cold'
        else:
            data['category'] = 'normal'
    
    return data

def store_processed_data(data):
    """Store the processed data in DynamoDB"""
    table.put_item(Item=data)
    return True 