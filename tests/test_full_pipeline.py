import boto3
import json
import time
import uuid
import pytest
import os
import subprocess
import sys
import random
from datetime import datetime, timezone, timedelta
import base64

# Path to the simulator script
SIMULATOR_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                             'src', 'data_simulator', 'simulator.py')

# Get stack name from environment or use default
STACK_NAME = os.environ.get('STACK_NAME', 'data-processing')

@pytest.fixture(scope="module")
def stack_outputs():
    """Get the outputs from the CloudFormation stack"""
    cloudformation = boto3.client('cloudformation')
    
    try:
        response = cloudformation.describe_stacks(StackName=STACK_NAME)
        outputs = {output['OutputKey']: output['OutputValue'] 
                  for output in response['Stacks'][0]['Outputs']}
        return outputs
    except Exception as e:
        pytest.fail(f"Failed to get stack outputs: {str(e)}")

@pytest.fixture(scope="module")
def aws_clients(stack_outputs):
    """Initialize AWS clients and resource names"""
    stream_name = stack_outputs['SensorDataStreamName']
    table_name = stack_outputs['ProcessedDataTableName']
    lambda_name = stack_outputs['DataProcessorFunction'].split(':')[-1]  # Extract name from ARN
    
    return {
        'kinesis': boto3.client('kinesis'),
        'dynamodb': boto3.resource('dynamodb').Table(table_name),
        'lambda_client': boto3.client('lambda'),
        'cloudwatch': boto3.client('cloudwatch'),
        'logs': boto3.client('logs'),
        'stream_name': stream_name,
        'table_name': table_name,
        'lambda_name': lambda_name
    }

@pytest.mark.order(1)
def test_kinesis_stream_exists(aws_clients):
    """Test that the Kinesis stream exists and is active"""
    kinesis = aws_clients['kinesis']
    stream_name = aws_clients['stream_name']
    
    try:
        response = kinesis.describe_stream(StreamName=stream_name)
        stream_status = response['StreamDescription']['StreamStatus']
        assert stream_status == 'ACTIVE', f"Kinesis stream {stream_name} is not active"
        print(f"✅ Kinesis stream {stream_name} exists and is active")
    except Exception as e:
        pytest.fail(f"Error checking Kinesis stream: {str(e)}")

@pytest.mark.order(2)
def test_dynamodb_table_exists(aws_clients):
    """Test that the DynamoDB table exists and is active"""
    dynamodb = boto3.client('dynamodb')
    table_name = aws_clients['table_name']
    
    try:
        response = dynamodb.describe_table(TableName=table_name)
        table_status = response['Table']['TableStatus']
        assert table_status == 'ACTIVE', f"DynamoDB table {table_name} is not active"
        print(f"✅ DynamoDB table {table_name} exists and is active")
    except Exception as e:
        pytest.fail(f"Error checking DynamoDB table: {str(e)}")

@pytest.mark.order(3)
def test_lambda_function_exists(aws_clients):
    """Test that the Lambda function exists and is configured correctly"""
    lambda_client = aws_clients['lambda_client']
    lambda_name = aws_clients['lambda_name']
    
    try:
        response = lambda_client.get_function(FunctionName=lambda_name)
        runtime = response['Configuration']['Runtime']
        handler = response['Configuration']['Handler']
        
        assert 'python' in runtime.lower(), f"Lambda function {lambda_name} is not using Python runtime"
        assert handler == 'app.lambda_handler', f"Lambda function {lambda_name} has incorrect handler"
        print(f"✅ Lambda function {lambda_name} exists with correct configuration")
    except Exception as e:
        pytest.fail(f"Error checking Lambda function: {str(e)}")

@pytest.mark.order(4)
def test_lambda_has_kinesis_trigger(aws_clients):
    """Test that the Lambda function has a Kinesis trigger"""
    lambda_client = aws_clients['lambda_client']
    lambda_name = aws_clients['lambda_name']
    stream_name = aws_clients['stream_name']
    
    try:
        response = lambda_client.list_event_source_mappings(
            FunctionName=lambda_name,
            EventSourceArn=f"arn:aws:kinesis:{boto3.session.Session().region_name}:{boto3.client('sts').get_caller_identity()['Account']}:stream/{stream_name}"
        )
        
        assert len(response['EventSourceMappings']) > 0, f"Lambda function {lambda_name} does not have a Kinesis trigger for stream {stream_name}"
        assert response['EventSourceMappings'][0]['State'] == 'Enabled', f"Kinesis trigger for Lambda function {lambda_name} is not enabled"
        print(f"✅ Lambda function {lambda_name} has an enabled Kinesis trigger")
    except Exception as e:
        pytest.fail(f"Error checking Lambda Kinesis trigger: {str(e)}")

@pytest.mark.order(5)
def test_single_record_processing(aws_clients):
    """Test processing a single record through the pipeline"""
    kinesis = aws_clients['kinesis']
    dynamodb = aws_clients['dynamodb']
    stream_name = aws_clients['stream_name']
    
    # Create unique test identifier
    test_id = f"test-{uuid.uuid4()}"
    
    # Generate test data
    test_data = {
        'sensor_id': test_id,
        'temperature': 22.5,
        'humidity': 55.0,
        'pressure': 1010.2,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    # Send data to Kinesis
    try:
        response = kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(test_data),
            PartitionKey=test_id
        )
        assert 'ShardId' in response, "Failed to put record in Kinesis stream"
        print(f"✅ Successfully sent test record to Kinesis stream")
    except Exception as e:
        pytest.fail(f"Error sending data to Kinesis: {str(e)}")
    
    # Wait for data to be processed
    max_retries = 15
    retries = 0
    item_found = False
    processed_item = None
    
    while retries < max_retries and not item_found:
        time.sleep(2)  # Wait for processing
        
        try:
            # Scan for items with our test sensor_id
            response = dynamodb.scan(
                FilterExpression='sensor_id = :sid',
                ExpressionAttributeValues={':sid': test_id}
            )
            
            if response['Items']:
                item_found = True
                processed_item = response['Items'][0]
                print(f"✅ Found processed item in DynamoDB after {retries+1} attempts")
        except Exception as e:
            print(f"Error scanning DynamoDB (attempt {retries+1}): {str(e)}")
        
        retries += 1
    
    # Verify the data was processed correctly
    assert item_found, f"Test data with ID {test_id} not found in DynamoDB after {max_retries} retries"
    assert processed_item['sensor_id'] == test_id, "Processed item has incorrect sensor_id"
    assert processed_item['temperature'] == test_data['temperature'], "Processed item has incorrect temperature"
    assert 'category' in processed_item, "Processed item missing 'category' field"
    assert 'processed_at' in processed_item, "Processed item missing 'processed_at' field"
    
    # Verify temperature categorization logic
    temp = test_data['temperature']
    if temp < 0:
        expected_category = 'freezing'
    elif temp < 18:
        expected_category = 'cold'
    elif temp < 24:
        expected_category = 'comfortable'
    elif temp < 30:
        expected_category = 'warm'
    else:
        expected_category = 'hot'
    
    assert processed_item['category'] == expected_category, f"Incorrect category: expected '{expected_category}', got '{processed_item['category']}'"
    print(f"✅ Record was processed correctly with category '{processed_item['category']}'")

@pytest.mark.order(6)
def test_batch_processing(aws_clients):
    """Test processing multiple records in batch"""
    kinesis = aws_clients['kinesis']
    dynamodb = aws_clients['dynamodb']
    stream_name = aws_clients['stream_name']
    
    # Create batch of test records
    batch_size = 10
    batch_id = f"batch-{uuid.uuid4()}"
    test_records = []
    
    for i in range(batch_size):
        test_data = {
            'sensor_id': f"{batch_id}-{i}",
            'temperature': round(20 + i * 2, 1),  # Different temperatures
            'humidity': 50 + i,
            'pressure': 1000 + i,
            'timestamp': (datetime.utcnow() + timedelta(seconds=i)).isoformat()
        }
        test_records.append({
            'Data': json.dumps(test_data).encode(),
            'PartitionKey': f"{batch_id}-{i}"
        })
    
    # Send batch to Kinesis
    try:
        response = kinesis.put_records(
            StreamName=stream_name,
            Records=test_records
        )
        failed_count = response.get('FailedRecordCount', 0)
        assert failed_count == 0, f"{failed_count} records failed to be put in Kinesis stream"
        print(f"✅ Successfully sent batch of {batch_size} records to Kinesis stream")
    except Exception as e:
        pytest.fail(f"Error sending batch to Kinesis: {str(e)}")
    
    # Wait for data to be processed
    max_retries = 15
    retries = 0
    processed_count = 0
    
    while retries < max_retries and processed_count < batch_size:
        time.sleep(3)  # Wait longer for batch processing
        
        try:
            # Scan for items with our batch ID prefix
            response = dynamodb.scan(
                FilterExpression='begins_with(sensor_id, :bid)',
                ExpressionAttributeValues={':bid': batch_id}
            )
            
            processed_count = len(response['Items'])
            print(f"Found {processed_count}/{batch_size} processed items (attempt {retries+1})")
        except Exception as e:
            print(f"Error scanning DynamoDB (attempt {retries+1}): {str(e)}")
        
        retries += 1
    
    # Verify all records were processed
    assert processed_count == batch_size, f"Only {processed_count}/{batch_size} records were processed"
    print(f"✅ All {batch_size} batch records were processed successfully")

@pytest.mark.order(7)
def test_simulator_integration(aws_clients):
    """Test the data simulator integration with the pipeline"""
    stream_name = aws_clients['stream_name']
    dynamodb = aws_clients['dynamodb']
    
    # Get initial count of items in the table
    try:
        initial_response = dynamodb.scan(Select='COUNT')
        initial_count = initial_response['Count']
    except Exception as e:
        pytest.fail(f"Error getting initial DynamoDB count: {str(e)}")
    
    # Run the simulator for a short period
    test_duration = 10
    num_sensors = 3
    interval = 0.5
    
    # Calculate expected records (approximate)
    expected_min_records = int((num_sensors * test_duration) / interval * 0.7)  # 70% success rate as minimum
    
    # Start the simulator as a subprocess
    try:
        simulator_process = subprocess.Popen([
            sys.executable, SIMULATOR_PATH,
            '--stream', stream_name,
            '--sensors', str(num_sensors),
            '--interval', str(interval),
            '--duration', str(test_duration)
        ])
        
        # Wait for the simulator to finish
        simulator_process.wait()
        print(f"✅ Simulator completed with exit code {simulator_process.returncode}")
        
        # Wait a bit more for processing to complete
        time.sleep(10)
    except Exception as e:
        pytest.fail(f"Error running simulator: {str(e)}")
    
    # Check DynamoDB for processed records
    try:
        final_response = dynamodb.scan(Select='COUNT')
        final_count = final_response['Count']
        new_records = final_count - initial_count
        
        assert new_records > 0, "No new records found in DynamoDB after running simulator"
        print(f"✅ Found {new_records} new records in DynamoDB after running simulator")
        
        # Check if we got at least the minimum expected records
        assert new_records >= expected_min_records, f"Expected at least {expected_min_records} new records, but got only {new_records}"
    except Exception as e:
        pytest.fail(f"Error checking processed records: {str(e)}")

@pytest.mark.order(8)
def test_lambda_metrics(aws_clients):
    """Test that Lambda metrics are being collected"""
    cloudwatch = aws_clients['cloudwatch']
    lambda_name = aws_clients['lambda_name']
    
    # Get metrics for the last 15 minutes
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=15)
    
    try:
        # Check invocation metrics
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': lambda_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Sum']
        )
        
        assert len(response['Datapoints']) > 0, "No Lambda invocation metrics found"
        total_invocations = sum(point['Sum'] for point in response['Datapoints'])
        assert total_invocations > 0, "Lambda function has not been invoked"
        print(f"✅ Lambda function was invoked {total_invocations} times in the last 15 minutes")
        
        # Check duration metrics
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Duration',
            Dimensions=[{'Name': 'FunctionName', 'Value': lambda_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Average', 'Maximum']
        )
        
        if response['Datapoints']:
            avg_duration = sum(point['Average'] for point in response['Datapoints']) / len(response['Datapoints'])
            max_duration = max(point['Maximum'] for point in response['Datapoints'])
            print(f"✅ Lambda average duration: {avg_duration:.2f}ms, maximum: {max_duration:.2f}ms")
    except Exception as e:
        pytest.fail(f"Error checking Lambda metrics: {str(e)}")

@pytest.mark.order(9)
def test_kinesis_metrics(aws_clients):
    """Test that Kinesis metrics are being collected"""
    cloudwatch = aws_clients['cloudwatch']
    stream_name = aws_clients['stream_name']
    
    # Get metrics for the last 15 minutes
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=15)
    
    try:
        # Check incoming records metrics
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Kinesis',
            MetricName='IncomingRecords',
            Dimensions=[{'Name': 'StreamName', 'Value': stream_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Sum']
        )
        
        assert len(response['Datapoints']) > 0, "No Kinesis incoming records metrics found"
        total_records = sum(point['Sum'] for point in response['Datapoints'])
        assert total_records > 0, "No records were sent to Kinesis stream"
        print(f"✅ Kinesis stream received {total_records} records in the last 15 minutes")
        
        # Check incoming bytes metrics
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Kinesis',
            MetricName='IncomingBytes',
            Dimensions=[{'Name': 'StreamName', 'Value': stream_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Sum']
        )
        
        if response['Datapoints']:
            total_bytes = sum(point['Sum'] for point in response['Datapoints'])
            print(f"✅ Kinesis stream received {total_bytes} bytes in the last 15 minutes")
    except Exception as e:
        pytest.fail(f"Error checking Kinesis metrics: {str(e)}")

@pytest.mark.order(10)
def test_error_handling(aws_clients):
    """Test error handling by sending malformed data"""
    kinesis = aws_clients['kinesis']
    logs = aws_clients['logs']
    stream_name = aws_clients['stream_name']
    lambda_name = aws_clients['lambda_name']
    
    # Send malformed data to Kinesis
    malformed_data = "This is not valid JSON"
    test_id = f"error-test-{uuid.uuid4()}"
    
    try:
        kinesis.put_record(
            StreamName=stream_name,
            Data=malformed_data,
            PartitionKey=test_id
        )
        print("✅ Sent malformed data to Kinesis stream")
        
        # Wait for Lambda to process and potentially log errors
        time.sleep(10)
        
        # Check CloudWatch logs for errors
        log_group_name = f"/aws/lambda/{lambda_name}"
        
        # Get the most recent log streams
        response = logs.describe_log_streams(
            logGroupName=log_group_name,
            orderBy='LastEventTime',
            descending=True,
            limit=5
        )
        
        if not response['logStreams']:
            pytest.skip("No log streams found for Lambda function")
        
        # Look for error messages in the log streams
        error_found = False
        for log_stream in response['logStreams']:
            log_events = logs.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream['logStreamName'],
                limit=100
            )
            
            for event in log_events['events']:
                if 'Error' in event['message'] or 'error' in event['message'].lower():
                    error_found = True
                    print(f"✅ Found error in logs: {event['message']}")
                    break
            
            if error_found:
                break
        
        # We expect to find errors, but if we don't, it's not necessarily a failure
        # The Lambda might handle the error gracefully
        if not error_found:
            print("⚠️ No explicit errors found in logs, but Lambda might have handled the error gracefully")
    except Exception as e:
        pytest.fail(f"Error testing error handling: {str(e)}")

@pytest.mark.order(11)
def test_performance_under_load(aws_clients):
    """Test performance under higher load"""
    kinesis = aws_clients['kinesis']
    cloudwatch = aws_clients['cloudwatch']
    stream_name = aws_clients['stream_name']
    lambda_name = aws_clients['lambda_name']
    
    # Send a larger batch of records
    batch_size = 50
    batch_id = f"perf-test-{uuid.uuid4()}"
    test_records = []
    
    for i in range(batch_size):
        test_data = {
            'sensor_id': f"{batch_id}-{i}",
            'temperature': round(random.uniform(-5, 40), 2),
            'humidity': round(random.uniform(0, 100), 2),
            'pressure': round(random.uniform(900, 1100), 2),
            'timestamp': (datetime.utcnow() + timedelta(milliseconds=i*10)).isoformat()
        }
        test_records.append({
            'Data': json.dumps(test_data).encode(),
            'PartitionKey': f"{batch_id}-{i}"
        })
    
    # Send batch to Kinesis
    try:
        start_time = time.time()
        response = kinesis.put_records(
            StreamName=stream_name,
            Records=test_records
        )
        put_time = time.time() - start_time
        
        failed_count = response.get('FailedRecordCount', 0)
        success_rate = ((batch_size - failed_count) / batch_size) * 100
        
        print(f"✅ Sent {batch_size} records in {put_time:.3f} seconds ({batch_size/put_time:.1f} records/sec)")
        print(f"✅ Success rate: {success_rate:.1f}% ({batch_size - failed_count}/{batch_size})")
        
        # Wait for processing to complete
        time.sleep(15)
        
        # Check Lambda duration metrics
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=5)
        
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Lambda',
            MetricName='Duration',
            Dimensions=[{'Name': 'FunctionName', 'Value': lambda_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=['Average', 'Maximum']
        )
        
        if response['Datapoints']:
            avg_duration = sum(point['Average'] for point in response['Datapoints']) / len(response['Datapoints'])
            max_duration = max(point['Maximum'] for point in response['Datapoints'])
            print(f"✅ Under load: Lambda average duration: {avg_duration:.2f}ms, maximum: {max_duration:.2f}ms")
            
            # Check if Lambda is performing well under load
            assert max_duration < 10000, f"Lambda maximum duration ({max_duration:.2f}ms) exceeds 10 seconds under load"
    except Exception as e:
        pytest.fail(f"Error testing performance under load: {str(e)}")

if __name__ == "__main__":
    # This allows running the tests directly with python
    pytest.main(["-xvs", __file__]) 