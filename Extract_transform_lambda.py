import boto3
import json
import csv
from datetime import datetime
from collections import Counter

s3 = boto3.client('s3')

ssm_client = boto3.client('ssm', region_name='eu-west-1') #added region due to NoRegionError

ssm_env_var_name = 'SSM_PARAMETER_NAME'

sqs = boto3.client('sqs')
queue_url = 'https://sqs.eu-west-1.amazonaws.com/992382716453/nubi-queue'

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        # Download the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_file = response['Body'].read().decode('utf-8')
    
    data_list = csv_to_list(csv_file)
    transformed_data = remove_sensitive_data(data_list)
    transformed_data = split_date_and_time(transformed_data)
    transformed_data = split_items_and_count_quantity(transformed_data)
    
    print('Lambda extract and tranformed processing completed.')
    
    sqs.send_message(
         QueueUrl=queue_url,
         MessageBody=json.dumps(transformed_data)
     )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data processing complete')
    }

def csv_to_list(csv_file):
    data_list = []
    column_names = ['date_time', 'location', 'customer_name', 'items', 'total_spent', 'payment_method', 'card_number']
    
    csv_reader = csv.DictReader(csv_file.splitlines(), fieldnames=column_names)
    for row in csv_reader:
        data_list.append(row)
    return data_list

def remove_sensitive_data(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        transformed_data.append({
            'date_time': data_dict['date_time'],
            'location': data_dict['location'],
            'items': data_dict['items'],
            'total_spent': data_dict['total_spent'],
            'payment_method': data_dict['payment_method']
        })
    print('Removed sensitive data:')
    return transformed_data

def split_date_and_time(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        date_time = data_dict['date_time']
        transaction_date, transaction_time = date_time.split(' ', 1)
        transaction_date = datetime.strptime(transaction_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        
        transformed_data.append({
            'date_time': date_time,
            'transaction_date': transaction_date,
            'transaction_time': transaction_time,
            'location': data_dict['location'],
            'items': data_dict['items'],
            'total_spent': data_dict['total_spent'],
            'payment_method': data_dict['payment_method']
        })
    print('split_date_and_time:')
    return transformed_data

def split_items_and_count_quantity(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        items = data_dict['items'].split(',')
        item_counts = Counter()
        item_list = []
        
        for item in items:
            product_name, product_price = item.rsplit(' - ', 1)
            product_name = product_name.strip()
            product_price = float(product_price.strip())
            item_counts[product_name] += 1
            item_list.append((product_name, product_price))
        
        for product_name, product_price in item_list:
            transformed_data.append({
                'transaction_date': data_dict['transaction_date'],
                'transaction_time': data_dict['transaction_time'],
                'location': data_dict['location'],
                'product_name': product_name,
                'product_price': product_price,
                'quantity': item_counts[product_name],
                'total_spent': float(data_dict['total_spent']),
                'payment_method': data_dict['payment_method']
            })
    print('split_items_and_count_quantity:')
    return transformed_data
