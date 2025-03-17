import boto3
import json
import time
import uuid
import pytest
import os
import subprocess
import sys
from datetime import datetime

# Path to the simulator script
SIMULATOR_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                             'src', 'data_simulator', 'simulator.py')

@pytest.fixture(scope="module")
def stack_outputs():
    """Get the outputs from the CloudFormation stack"""
    cloudformation = boto3.client('cloudformation')
    stack_name = os.environ.get('STACK_NAME', 'real-time-pipeline')
    
    try:
        response = cloudformation.describe_stacks(StackName=stack_name)
        outputs = {output['OutputKey']: output['OutputValue'] 
                  for output in response['Stacks'][0]['Outputs']}
        return outputs
    except Exception as e:
        pytest.fail(f"Failed to get stack outputs: {str(e)}")

def test_end_to_end_pipeline(stack_outputs):
    """Test the entire pipeline from data generation to storage"""
    # Get resource names from stack outputs
    stream_name = stack_outputs['SensorDataStreamName']
    table_name = stack_outputs['ProcessedDataTableName']
    
    # Create unique test identifier
    test_id = f"test-{uuid.uuid4()}"
    
    # Initialize AWS clients
    kinesis = boto3.client('kinesis')
    dynamodb = boto3.resource('dynamodb').Table(table_name)
    
    # Generate and send test data
    test_data = {
        'sensor_id': test_id,
        'temperature': 22.5,
        'humidity': 55.0,
        'pressure': 1010.2,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    # Send data to Kinesis
    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(test_data),
        PartitionKey=test_id
    )
    
    # Wait for data to be processed (Lambda trigger and DynamoDB write)
    max_retries = 10
    retries = 0
    item_found = False
    
    while retries < max_retries and not item_found:
        # Query DynamoDB for the processed item
        time.sleep(2)  # Wait for processing
        
        # Scan for items with our test sensor_id
        response = dynamodb.scan(
            FilterExpression='sensor_id = :sid',
            ExpressionAttributeValues={':sid': test_id}
        )
        
        if response['Items']:
            item_found = True
            processed_item = response['Items'][0]
        
        retries += 1
    
    # Verify the data was processed and stored correctly
    assert item_found, f"Test data with ID {test_id} not found in DynamoDB after {max_retries} retries"
    
    # Verify data processing logic was applied
    assert processed_item['sensor_id'] == test_id
    assert processed_item['temperature'] == test_data['temperature']
    assert 'category' in processed_item
    assert 'processed_at' in processed_item

def test_simulator_integration(stack_outputs):
    """Test the data simulator integration with the pipeline"""
    # Get stream name from stack outputs
    stream_name = stack_outputs['SensorDataStreamName']
    table_name = stack_outputs['ProcessedDataTableName']
    
    # Run the simulator for a short period (10 seconds)
    test_duration = 10
    num_sensors = 2
    
    # Start the simulator as a subprocess
    simulator_process = subprocess.Popen([
        sys.executable, SIMULATOR_PATH,
        '--stream', stream_name,
        '--sensors', str(num_sensors),
        '--interval', '0.5',
        '--duration', str(test_duration)
    ])
    
    # Wait for the simulator to finish
    simulator_process.wait()
    
    # Wait a bit more for processing to complete
    time.sleep(5)
    
    # Check DynamoDB for processed records
    dynamodb = boto3.resource('dynamodb').Table(table_name)
    
    # Get the count of items in the table (this is a simple approach - in production
    # you might want to use a more efficient query based on timestamps)
    response = dynamodb.scan(Select='COUNT')
    item_count = response['Count']
    
    # We should have at least some records (exact count may vary due to timing)
    assert item_count > 0, "No processed records found in DynamoDB"
    print(f"Found {item_count} processed records in DynamoDB") 