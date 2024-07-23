import io
import csv
import json
import boto3
from datetime import datetime
from collections import Counter

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

output_bucket_name = 'nubi-bucket-2'
queue_url = 'https://sqs.eu-west-1.amazonaws.com/992382716453/nubi-queue'

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        # Download the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_file = response['Body'].read().decode('utf-8')
    
    data_list = csv_to_list(csv_file)
    transformed_data = transform_data(data_list)
    
    s3_object_key = save_transformed_data_to_s3_as_csv(transformed_data, object_key)
    send_sqs_message(s3_object_key)
    
    print('Lambda extract and transform processing completed.')
    
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

def transform_data(data_list):
    transformed_data = []
    for data_dict in data_list:
        date_time = data_dict['date_time']
        transaction_date, transaction_time = date_time.split(' ', 1)
        transaction_date = datetime.strptime(transaction_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        
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
                'transaction_date': transaction_date,
                'transaction_time': transaction_time,
                'location': data_dict['location'],
                'product_name': product_name,
                'product_price': product_price,
                'quantity': item_counts[product_name],
                'total_spent': float(data_dict['total_spent']),
                'payment_method': data_dict['payment_method']
            })
    return transformed_data

def save_transformed_data_to_s3_as_csv(transformed_data, original_key):
    output = io.StringIO()
    csv_writer = csv.DictWriter(output, fieldnames=transformed_data[0].keys())
    csv_writer.writeheader()
    csv_writer.writerows(transformed_data)
    csv_data = output.getvalue()
    
    new_object_key = f'transformed/{original_key.split("/")[-1].replace(".csv", "_transformed.csv")}'
    
    s3.put_object(Bucket=output_bucket_name, Key=new_object_key, Body=csv_data)
    print(f'Transformed data saved as CSV to {output_bucket_name}/{new_object_key}')
    return new_object_key

def send_sqs_message(s3_object_key):
    message_body = {
        'bucket': output_bucket_name,
        'key': s3_object_key
    }
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_body)
    )
    print(f'SQS message sent for {s3_object_key}')
