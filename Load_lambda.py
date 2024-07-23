import os
import boto3
import json
import csv
import psycopg2 as psy

s3 = boto3.client('s3')

ssm_client = boto3.client('ssm', region_name='eu-west-1') #added region due to NoRegionError

ssm_env_var_name = 'SSM_PARAMETER_NAME'
sqs = boto3.client('sqs')
queue_url = 'https://sqs.eu-west-1.amazonaws.com/992382716453/nubi-queue'

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        
    
        cur = None
        conn = None
        try:
            # Process SQS message
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            csv_file = response['Body'].read().decode('utf-8')            
        
            transformed_data= csv_to_list(csv_file)
           
            
            # connection
            nubi_redshift_settings = os.environ['SSM_PARAMETER_NAME']
            redshift_details = get_ssm_param(nubi_redshift_settings)
            
            conn, cur = open_sql_database_connection_and_cursor(redshift_details)
            
            # load data
            insert_products(cur, transformed_data)
            insert_locations(cur, transformed_data)
            insert_transactions(cur, transformed_data)
            insert_orders(cur, transformed_data)
            
            # Commit changes to database
            conn.commit()
            
            # Delete SQS message
           # sqs.delete_message(
               # QueueUrl=queue_url,
                #ReceiptHandle=record['receiptHandle']
            
            
            print('Processing and deletion completed successfully.')
        
        except Exception as whoopsy:
            print(f'lambda_handler: failure, error={whoopsy}')
            continue
        
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data Loaded')
    }

def get_ssm_param(param_name):
    parameter_details = ssm_client.get_parameter(Name=param_name)
    redshift_details = json.loads(parameter_details['Parameter']['Value'])

    return redshift_details

def open_sql_database_connection_and_cursor(redshift_details):
    db_connection = psy.connect(host=redshift_details['host'],
                                database=redshift_details['database-name'],
                                user=redshift_details['user'],
                                password=redshift_details['password'],
                                port=redshift_details['port'])
    cursor = db_connection.cursor()
    return db_connection, cursor

def csv_to_list(csv_file):
    data_list = []
    csv_reader = csv.DictReader(csv_file.splitlines())
    for row in csv_reader:
        data_list.append(row)
    return data_list




def fetch_location_id(cursor, location_name):
    cursor.execute("SELECT location_id FROM Location WHERE location_name = %s", (location_name,))
    result = cursor.fetchone()
    return result[0] if result else None


def fetch_product_id(cursor, product_name, product_price):
    cursor.execute("SELECT product_id FROM Products WHERE product_name = %s AND product_price = %s", (product_name, product_price))
    result = cursor.fetchone()
    return result[0] if result else None

def fetch_transaction_id(cursor, transaction_date, transaction_time, location_id):
    cursor.execute("""
        SELECT transaction_id 
        FROM Transactions 
        WHERE transaction_date = %s 
        AND transaction_time = %s 
        AND location_id = %s
        """, (transaction_date, transaction_time, location_id))
    result = cursor.fetchone()
    return result[0] if result else None

def insert_locations(cursor, transformed_data):
    locations_to_insert = []
    for data_dict in transformed_data:
        location_name = data_dict['location']
        if location_name:
            cursor.execute("SELECT location_id FROM Location WHERE location_name = %s", (location_name,))
            if cursor.fetchone() is None and (location_name,) not in locations_to_insert:
                locations_to_insert.append((location_name,))
    
    if locations_to_insert:
        cursor.executemany("INSERT INTO Location (location_name) VALUES (%s)", locations_to_insert)
        print("insert_location completed sucessfully")
        
def insert_products(cursor, transformed_data):
    products_to_insert = []
    for data_dict in transformed_data:
        product_name = data_dict['product_name']
        product_price = data_dict['product_price']
        cursor.execute("SELECT product_id FROM Products WHERE product_name = %s AND product_price = %s", (product_name, product_price))
        if cursor.fetchone() is None and (product_name, product_price) not in products_to_insert:
            products_to_insert.append((product_name, product_price))
    
    if products_to_insert:
        cursor.executemany("INSERT INTO Products (product_name, product_price) VALUES (%s, %s)", products_to_insert)
        print("insert_products completed sucessfully")

def insert_transactions(cursor, transformed_data):
    transactions_to_insert = []
    for data_dict in transformed_data:
        location_name = data_dict['location']
        transaction_date = data_dict['transaction_date']
        transaction_time = data_dict['transaction_time']
        payment_method = data_dict['payment_method']
        total_spent = data_dict['total_spent']
        
        location_id = fetch_location_id(cursor, location_name)
        if location_id:
            transaction_id = fetch_transaction_id(cursor, transaction_date, transaction_time, location_id)
            if transaction_id is None and (transaction_date, transaction_time, location_id, payment_method, total_spent) not in transactions_to_insert:
                transactions_to_insert.append((transaction_date, transaction_time, location_id, payment_method, total_spent))
    
    if transactions_to_insert:
        cursor.executemany("INSERT INTO Transactions (transaction_date, transaction_time, location_id, payment_method, total_spent) VALUES (%s, %s, %s, %s, %s)", transactions_to_insert)
        print("insert_transaction completed sucessfully")

def insert_orders(cursor, transformed_data):
    orders_to_insert = []
    for data_dict in transformed_data:
        product_name = data_dict['product_name']
        product_price = data_dict['product_price']
        transaction_date = data_dict['transaction_date']
        transaction_time = data_dict['transaction_time']
        quantity = data_dict['quantity']
        
        product_id = fetch_product_id(cursor, product_name, product_price)
        location_id = fetch_location_id(cursor, data_dict['location'])
        transaction_id = fetch_transaction_id(cursor, transaction_date, transaction_time, location_id)
        
        if product_id and transaction_id:
            cursor.execute("SELECT order_id FROM Orders WHERE transaction_id = %s AND product_id = %s", (transaction_id, product_id))
            if cursor.fetchone() is None and (transaction_id, product_id, quantity) not in orders_to_insert:
                orders_to_insert.append((transaction_id, product_id, quantity))
    
    if orders_to_insert:
        cursor.executemany("INSERT INTO Orders (transaction_id, product_id, quantity) VALUES (%s, %s, %s)", orders_to_insert)
        print("insert_order completed sucessfully")


