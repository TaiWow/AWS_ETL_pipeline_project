import boto3
import csv
from datetime import datetime
import os
import psycopg2 as psy
import json
from collections import Counter


s3 = boto3.client('s3')

ssm_client = boto3.client('ssm', region_name='eu-west-1') #added region due to NoRegionError

ssm_env_var_name = 'SSM_PARAMETER_NAME'


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

    cur = None
    conn = None

    try:

        nubi_redshift_settings = os.environ[ssm_env_var_name]
        print(f'lambda_handler: nubi_redshift_settings={nubi_redshift_settings} from ssm_env_var_name={ssm_env_var_name}')

        # connection
        redshift_details = get_ssm_param(nubi_redshift_settings)
        conn, cur = open_sql_database_connection_and_cursor(redshift_details)
        
        # load data
        process_products_list(cur, transformed_data)
        process_locations(cur, transformed_data)
        process_transactions(cur, transformed_data)
        process_orders(cur, transformed_data)

        print(f'lambda_handler: done')

    except Exception as whoopsy:
        # ...exception reporting
        print(f'lambda_handler: failure, error=${whoopsy}')
        raise whoopsy
    
    finally:
        # Ensure cursor and connection are closed
        if cur:
            cur.close()
        if conn:
            conn.close()
       
        
def get_ssm_param(param_name):
    print(f'get_ssm_param: getting param_name={param_name}')
    parameter_details = ssm_client.get_parameter(Name=param_name)
    redshift_details = json.loads(parameter_details['Parameter']['Value'])

    # The JSON should have this structure:
    # {
    #   "database-name": "<team name>_cafe_db",
    #   "host": "<redshift address>.eu-west-1.redshift.amazonaws.com",
    #   "port": 5439,
    #   "password": "<password>",
    #   "user": "<team name>_user"
    # }

    host = redshift_details['host']
    user = redshift_details['user']
    db = redshift_details['database-name']
    print(f'get_ssm_param loaded for db={db}, user={user}, host={host}')
    return redshift_details

def open_sql_database_connection_and_cursor(redshift_details):
    try:
        print('open_sql_database_connection_and_cursor: new connection starting...')
        db_connection = psy.connect(host=redshift_details['host'],
                                    database=redshift_details['database-name'],
                                    user=redshift_details['user'],
                                    password=redshift_details['password'],
                                    port=redshift_details['port'])
        cursor = db_connection.cursor()
        print('open_sql_database_connection_and_cursor: connection ready')
        return db_connection, cursor
    except ConnectionError as ex:
        print(f'open_sql_database_connection_and_cursor: failed to open connection: {ex}')
        raise ex

def csv_to_list(csv_file):
    data_list = []
    column_names = ['date_time', 'location', 'customer_name', 'items', 'total_spent', 'payment_method', 'card_number']

    
    csv_reader = csv.DictReader(csv_file.splitlines(),fieldnames=column_names)
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
    return transformed_data

def insert_location(cursor, location_name):
    cursor.execute("SELECT location_id FROM Location WHERE location_name = %s", (location_name,))
    existing_location = cursor.fetchone()
    
    if existing_location:
        return existing_location[0]
        
    cursor.execute("""
        INSERT INTO location (location_name) VALUES (%s);
        SELECT location_id FROM Location ORDER BY location_id DESC;
    """, (location_name,))
    
    location_id = cursor.fetchone()[0]
    return location_id

def process_locations(cursor, transformed_data):
    for data_dict in transformed_data:
        location_name = data_dict['location']
        insert_location(cursor, location_name)
    
    cursor.connection.commit()

def insert_product(cursor, product_name, product_price):
    
        cursor.execute("SELECT product_id FROM Products WHERE product_name = %s AND product_price = %s", (product_name, product_price))
        existing_product = cursor.fetchone()
        
        if existing_product:
            return existing_product[0]
        
        cursor.execute("""
            INSERT INTO Products (product_name, product_price) VALUES (%s, %s);
            SELECT product_id FROM Products ORDER BY product_id DESC;
        """, (product_name, product_price))
        
        product_id = cursor.fetchone()[0]
        return product_id
    
def process_products_list(cursor, transformed_data):
    for data_dict in transformed_data:
        product_name = data_dict['product_name']
        product_price = data_dict['product_price']
        insert_product(cursor, product_name, product_price)
    
    cursor.connection.commit()

def insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method,total_spent):
   
        location_id = insert_location(cursor, location_name)
        
        check_sql = """
            SELECT transaction_id FROM Transactions 
            WHERE transaction_date = %s AND transaction_time = %s AND location_id = %s AND payment_method = %s AND total_spent = %s 
        """
        cursor.execute(check_sql, (transaction_date, transaction_time, location_id, payment_method, total_spent))
        existing_transaction = cursor.fetchone()
        
        if existing_transaction:
            return existing_transaction[0]
        
    
        insert_sql = """
            INSERT INTO Transactions (transaction_date, transaction_time, location_id, payment_method,total_spent) 
            VALUES (%s, %s, %s, %s,%s);
            SELECT transaction_id FROM Transactions ORDER BY transaction_id DESC;
        """
        cursor.execute(insert_sql, (transaction_date, transaction_time, location_id, payment_method,total_spent))
        transaction_id = cursor.fetchone()[0]
        return transaction_id
    
def process_transactions(cursor, transformed_data):
    for data_dict in transformed_data:
        try:
    
            transaction_date = data_dict['transaction_date']
            transaction_time = data_dict['transaction_time']
            location_name = data_dict['location']
            payment_method = data_dict['payment_method']
            total_spent = float(data_dict['total_spent'])
        
            insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method,total_spent)
            cursor.connection.commit()
            
        except KeyError as e:
            print(f"Missing transaction data, skipping entry: {str(e)}")
            continue  # Skip the current transaction
        
        # Handling any other exceptions that may occur during insertion
        except Exception as e:
            print(f"Error inserting transaction: {str(e)}")
            continue  # Skip the current transaction

def insert_order(cursor, quantity, product_name, product_price, transaction_date, transaction_time, location_name, payment_method, total_spent):
    
    transaction_id = insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent)
    
    product_id = insert_product(cursor, product_name, product_price)
        
    cursor.execute("SELECT order_id FROM Orders WHERE transaction_id = %s AND product_id = %s", (transaction_id, product_id))
    existing_order = cursor.fetchone()
        
    if existing_order:
        return existing_order[0]
        
    cursor.execute("""
        INSERT INTO Orders (transaction_id, product_id, quantity) 
        VALUES (%s, %s, %s); 
        SELECT order_id FROM Orders ORDER BY order_id DESC;
        """, (transaction_id, product_id, quantity))

    order_id = cursor.fetchone()[0]
    return order_id
    
def process_orders(cursor, transformed_data):
    for data_dict in transformed_data: 
        try:
            
            product_name = data_dict['product_name']
            product_price = data_dict['product_price']
            quantity = data_dict['quantity']
            transaction_date = data_dict['transaction_date']
            transaction_time = data_dict['transaction_time']
            location_name = data_dict['location']
            payment_method = data_dict['payment_method']
            total_spent = float(data_dict['total_spent'])
        
            insert_order(cursor, quantity, product_name, product_price, transaction_date, transaction_time, location_name, payment_method, total_spent)
            
            cursor.connection.commit()
        
        except KeyError as e:
            print(f"Missing key {str(e)}, skipping entry: {data_dict}")
            continue  

        except Exception as e:
            print(f"Error inserting order: {str(e)}")
            continue
  


