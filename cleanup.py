import boto3
import argparse
import time
import sys

def delete_stack(stack_name):
    """Delete the CloudFormation stack and all resources"""
    print(f"Deleting stack: {stack_name}")
    cloudformation = boto3.client('cloudformation')
    
    try:
        # Start stack deletion
        cloudformation.delete_stack(StackName=stack_name)
        
        # Wait for stack deletion to complete
        print("Waiting for stack deletion to complete...")
        waiter = cloudformation.get_waiter('stack_delete_complete')
        waiter.wait(StackName=stack_name, WaiterConfig={'Delay': 10, 'MaxAttempts': 30})
        
        print(f"Stack {stack_name} deleted successfully")
        return True
    except Exception as e:
        print(f"Error deleting stack: {str(e)}")
        return False

def collect_performance_metrics(stack_name):
    """Collect and display performance metrics from CloudWatch"""
    cloudwatch = boto3.client('cloudwatch')
    
    # Get the current time and 5 minutes ago
    end_time = time.time()
    start_time = end_time - 300  # 5 minutes
    
    # Get stack resources to find the Lambda function and Kinesis stream
    cloudformation = boto3.client('cloudformation')
    try:
        resources = cloudformation.list_stack_resources(StackName=stack_name)
        
        # Find Lambda function and Kinesis stream
        lambda_function = None
        kinesis_stream = None
        
        for resource in resources['StackResourceSummaries']:
            if resource['ResourceType'] == 'AWS::Lambda::Function':
                lambda_function = resource['PhysicalResourceId']
            elif resource['ResourceType'] == 'AWS::Kinesis::Stream':
                kinesis_stream = resource['PhysicalResourceId']
        
        if not lambda_function or not kinesis_stream:
            print("Could not find Lambda function or Kinesis stream in stack resources")
            return
        
        # Get Lambda metrics
        print("\n=== Lambda Function Metrics ===")
        lambda_metrics = [
            {'Name': 'Invocations', 'Unit': 'Count'},
            {'Name': 'Duration', 'Unit': 'Milliseconds'},
            {'Name': 'Errors', 'Unit': 'Count'},
            {'Name': 'Throttles', 'Unit': 'Count'}
        ]
        
        for metric in lambda_metrics:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/Lambda',
                MetricName=metric['Name'],
                Dimensions=[{'Name': 'FunctionName', 'Value': lambda_function}],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=['Sum', 'Average', 'Maximum']
            )
            
            if response['Datapoints']:
                # Sort datapoints by timestamp
                datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
                
                # Calculate sum and average
                sum_value = sum(dp['Sum'] for dp in datapoints if 'Sum' in dp)
                avg_value = sum(dp['Average'] for dp in datapoints if 'Average' in dp) / len(datapoints) if datapoints else 0
                max_value = max((dp['Maximum'] for dp in datapoints if 'Maximum' in dp), default=0)
                
                print(f"{metric['Name']}:")
                print(f"  Total: {sum_value:.2f} {metric['Unit']}")
                print(f"  Average: {avg_value:.2f} {metric['Unit']}")
                print(f"  Maximum: {max_value:.2f} {metric['Unit']}")
            else:
                print(f"{metric['Name']}: No data available")
        
        # Get Kinesis metrics
        print("\n=== Kinesis Stream Metrics ===")
        kinesis_metrics = [
            {'Name': 'IncomingRecords', 'Unit': 'Count'},
            {'Name': 'IncomingBytes', 'Unit': 'Bytes'},
            {'Name': 'GetRecords.IteratorAgeMilliseconds', 'Unit': 'Milliseconds'},
            {'Name': 'ReadProvisionedThroughputExceeded', 'Unit': 'Count'}
        ]
        
        for metric in kinesis_metrics:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/Kinesis',
                MetricName=metric['Name'],
                Dimensions=[{'Name': 'StreamName', 'Value': kinesis_stream}],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=['Sum', 'Average', 'Maximum']
            )
            
            if response['Datapoints']:
                # Sort datapoints by timestamp
                datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
                
                # Calculate sum and average
                sum_value = sum(dp['Sum'] for dp in datapoints if 'Sum' in dp)
                avg_value = sum(dp['Average'] for dp in datapoints if 'Average' in dp) / len(datapoints) if datapoints else 0
                max_value = max((dp['Maximum'] for dp in datapoints if 'Maximum' in dp), default=0)
                
                print(f"{metric['Name']}:")
                print(f"  Total: {sum_value:.2f} {metric['Unit']}")
                print(f"  Average: {avg_value:.2f} {metric['Unit']}")
                print(f"  Maximum: {max_value:.2f} {metric['Unit']}")
            else:
                print(f"{metric['Name']}: No data available")
                
    except Exception as e:
        print(f"Error collecting metrics: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Cleanup AWS resources and collect performance metrics')
    parser.add_argument('--stack-name', default='real-time-pipeline', help='CloudFormation stack name')
    parser.add_argument('--metrics-only', action='store_true', help='Only collect metrics, do not delete stack')
    
    args = parser.parse_args()
    
    # Collect performance metrics
    print("Collecting performance metrics...")
    collect_performance_metrics(args.stack_name)
    
    # Delete stack if not metrics-only mode
    if not args.metrics_only:
        if delete_stack(args.stack_name):
            print("Cleanup completed successfully")
        else:
            print("Cleanup failed")
            sys.exit(1)

if __name__ == "__main__":
    main() 