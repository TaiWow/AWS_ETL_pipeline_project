import boto3
import csv
from datetime import datetime


s3 = boto3.client('s3')

# lambda function to extract csv data from s3 bucket event  and transform - ideally should be saved back ito a clean bucket
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
    transformed_data = split_items_into_list(transformed_data)
       
        
 
def csv_to_list(csv_file):
    data_list = []
    column_names = ['date_time', 'location', 'customer_name', 'items', 'total_spent', 'payment_method', 'card_number']

    with open(csv_file, 'r') as file:
        csv_file = csv.DictReader(file, fieldnames=column_names)
        for row in csv_file:
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
    return transformed_data



def split_date_and_time(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        date_time = data_dict['date_time']
        transaction_date, transaction_time = date_time.split(' ', 1)
        
        # Convert transaction_date to YYYY-MM-DD format
        transaction_date = datetime.strptime(transaction_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        
        transformed_data.append({
            'date_time': date_time,  # Keep original for reference
            'transaction_date': transaction_date,
            'transaction_time': transaction_time,
            'location': data_dict['location'],
            'items': data_dict['items'],
            'total_spent': data_dict['total_spent'],  # Corrected key name here
            'payment_method': data_dict['payment_method']
        })
    return transformed_data


def split_items_into_list(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        items = data_dict['items'].split(',')
        item_list = []
        for item in items:
            product_name, product_price = item.rsplit(' - ', 1)
            product_name = product_name.strip()
            product_price = float(product_price.strip())
            item_list.append((product_name, product_price))
        transformed_data.append({
            'transaction_date': data_dict['transaction_date'],  # Use transformed fields from previous step
            'transaction_time': data_dict['transaction_time'],
            'location': data_dict['location'],
            'items': item_list,
            'total_spent': data_dict['total_spent'],
            'payment_method': data_dict['payment_method']
        })
    return transformed_data


        

