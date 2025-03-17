import json
import pytest
import os
import sys
import boto3
from unittest.mock import patch, MagicMock

# Add the Lambda function directory to the path
sys.path.append('src/data_processor')

# Import the Lambda handler
from app import lambda_handler, process_sensor_data, store_processed_data

# Mock environment variables
os.environ['TABLE_NAME'] = 'test-table'

# Sample Kinesis event
@pytest.fixture
def kinesis_event():
    return {
        'Records': [
            {
                'kinesis': {
                    'partitionKey': 'sensor-1',
                    'kinesisSchemaVersion': '1.0',
                    'data': 'eyJzZW5zb3JfaWQiOiAic2Vuc29yLTEiLCAidGVtcGVyYXR1cmUiOiAyNS41LCAiaHVtaWRpdHkiOiA2MC4yLCAicHJlc3N1cmUiOiAxMDEzLjIsICJ0aW1lc3RhbXAiOiAiMjAyMy0wNS0xNVQxMjozNDo1Ni43ODkxMjMifQ==',
                    'sequenceNumber': '49545115243490985018280067714973144582180062593244200961',
                    'approximateArrivalTimestamp': 1573147851.142
                },
                'eventSource': 'aws:kinesis',
                'eventVersion': '1.0',
                'eventID': 'shardId-000000000000:49545115243490985018280067714973144582180062593244200961',
                'eventName': 'aws:kinesis:record',
                'invokeIdentityArn': 'arn:aws:iam::123456789012:role/lambda-role',
                'awsRegion': 'us-east-1',
                'eventSourceARN': 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream'
            }
        ]
    }

# Test the process_sensor_data function
def test_process_sensor_data():
    # Test data with normal temperature
    data = {
        'sensor_id': 'sensor-1',
        'temperature': 25.5,
        'humidity': 60.2
    }
    
    result = process_sensor_data(data)
    
    assert result['sensor_id'] == 'sensor-1'
    assert result['temperature'] == 25.5
    assert result['category'] == 'normal'
    assert 'processed_at' in result
    assert 'id' in result
    
    # Test data with cold temperature
    data = {
        'sensor_id': 'sensor-2',
        'temperature': 5.0,
        'humidity': 40.0
    }
    
    result = process_sensor_data(data)
    assert result['category'] == 'cold'
    
    # Test data with negative temperature (should be filtered to 0)
    data = {
        'sensor_id': 'sensor-3',
        'temperature': -10.0,
        'humidity': 30.0
    }
    
    result = process_sensor_data(data)
    assert result['temperature'] == 0
    assert result['category'] == 'cold'

# Test the Lambda handler with mocked DynamoDB
@patch('app.table')
def test_lambda_handler(mock_table, kinesis_event):
    # Configure the mock
    mock_table.put_item.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
    
    # Call the Lambda handler
    response = lambda_handler(kinesis_event, {})
    
    # Verify the response
    assert response['statusCode'] == 200
    assert 'processed_records' in json.loads(response['body'])
    assert json.loads(response['body'])['processed_records'] == 1
    
    # Verify DynamoDB was called
    mock_table.put_item.assert_called_once()

# Test error handling
@patch('app.table')
def test_lambda_handler_error(mock_table, kinesis_event):
    # Configure the mock to raise an exception
    mock_table.put_item.side_effect = Exception("DynamoDB error")
    
    # Call the Lambda handler and expect an exception
    with pytest.raises(Exception):
        lambda_handler(kinesis_event, {}) 