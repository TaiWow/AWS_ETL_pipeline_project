import boto3
import csv
from datetime import datetime
import os
import psycopg2 as psy
import boto3
import json


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
    transformed_data = split_items_into_list(transformed_data)

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


def insert_location(cursor, location_name):
    # checks if location name already exists in the database
    cursor.execute("SELECT 1 FROM Location WHERE location_name = %s", (location_name,))
    if cursor.fetchone() is not None:
        print(f"Location '{location_name}' already exists. Skipping......")
        return  # Exit the function without inserting

    # Insert the location if it's unique and get the generated location_id
    cursor.execute("""
        INSERT INTO location (location_name) VALUES (%s);
        SELECT location_id FROM Location ORDER BY location_id DESC;
    """, (location_name,))
    location_id = cursor.fetchone()[0] 
    return location_id

def process_locations(cursor, transformed_data):
    location_list = []  # an empty list to keep track of processed locations
    
    for data_dict in transformed_data:
        location_name = data_dict['location']
        if location_name not in location_list:
            insert_location(cursor, location_name)  # Insert the location if it's not already processed
            location_list.append(location_name)  # Adds the location to the list of processed locations
    
    cursor.connection.commit()  

def get_location_id(cursor, location_name):
    # checks if location name already exists in the database
    cursor.execute("SELECT 1 FROM Location WHERE location_name = %s", (location_name,))
    if cursor.fetchone() is not None:
        cursor.execute("SELECT location_id FROM Location WHERE location_name = %s", (location_name,))
        location_id = cursor.fetchone()[0]
        return location_id 


def insert_product(cursor, product_name, product_price):
    
    # checks if product name already exists in the database
    cursor.execute("SELECT 1 FROM products WHERE product_name = %s", (product_name,))
    if cursor.fetchone() is not None:
        print(f"Product '{product_name}' already exists. Skipping......")
        print("---"*30)
        return   # exit the function without inserting if the product  exists

    product_sql = """
        INSERT INTO Products (product_name, product_price) VALUES (%s, %s);
        SELECT product_id FROM Products ORDER BY product_id DESC;
    """
   
    cursor.execute(product_sql, (product_name, product_price))
    # fetch the ID of the new product
    product_id = cursor.fetchone()[0]
    return product_id

def process_products_list(cursor, transformed_data):
   
    item_list = []  
    
    # loops over each dictionary in the transformed data
    for dict in transformed_data:
        # over each item in the 'items' list of the current ddict
        for item in dict['items']:
            # if not  in the processed list, insert to database
            if item not in item_list:
                insert_product(cursor, item[0], item[1])
                item_list.append(item)
    
    cursor.connection.commit()

def get_product_id(cursor, product_name):
    # checks if product name already exists in the database
    cursor.execute("SELECT 1 FROM products WHERE product_name = %s", (product_name,))
    if cursor.fetchone() is not None:
        cursor.execute("SELECT product_id FROM Products WHERE product_name = %s", (product_name,))
        product_id = cursor.fetchone()[0]
        return product_id


def insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent):
    
    location_id = get_location_id(cursor,location_name)
    check_sql = """
        SELECT 1 FROM Transactions 
        WHERE transaction_date = %s AND transaction_time = %s AND location_id = %s AND payment_method = %s
    """
    cursor.execute(check_sql, (transaction_date, transaction_time, location_id, payment_method))
    if cursor.fetchone() is not None:
        return 

    insert_sql = """
        INSERT INTO Transactions (transaction_date, transaction_time, location_id, payment_method, total_spent) VALUES (%s, %s, %s, %s, %s);
        SELECT transaction_id FROM Transactions ORDER BY transaction_id DESC;
    """
    cursor.execute(insert_sql, (transaction_date, transaction_time, location_id, payment_method, total_spent))
    transaction_id = cursor.fetchone()[0]
    return transaction_id


def process_transactions(cursor, transformed_data):
    transaction_ids = []
    
    # loop over each transaction data dictionary in the transformed data
    for data_dict in transformed_data:
        try:
            # extracting transaction  from the data dictionary
            transaction_date = data_dict['transaction_date']
            transaction_time = data_dict['transaction_time']
            location_name = data_dict['location']
            payment_method = data_dict['payment_method']
            total_spent = float(data_dict['total_spent'])  
            
            # get the transaction_id
            transaction_id = insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent)
            
            # if the transaction was inserted successfully, append the transaction_id to the list
            if transaction_id is not transaction_ids:
                transaction_ids.append(transaction_id) 
       
        except KeyError as e:
            print(f"Missing transaction data, skipping entry: {str(e)}")
            continue  # Skip the current transaction
        
        # Handling any other exceptions that may occur during insertion
        except Exception as e:
            print(f"Error inserting transaction: {str(e)}")
            continue  # Skip the current transaction

    cursor.connection.commit()
    
    return transaction_ids

def get_transaction_id(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent):
    location_id = get_location_id(cursor,location_name)
    check_sql = """
        SELECT 1 FROM Transactions 
        WHERE transaction_date = %s AND transaction_time = %s AND location_id = %s AND payment_method = %s
    """
    cursor.execute(check_sql, (transaction_date, transaction_time, location_id, payment_method))
    if cursor.fetchone() is not None:
        cursor.execute("SELECT transaction_id FROM Transactions WHERE transaction_date = %s AND transaction_time = %s AND location_id = %s AND payment_method = %s", (transaction_date, transaction_time, location_id, payment_method))
        transaction_id = cursor.fetchone()[0]
        return transaction_id       


def insert_order(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent, item):
    
    product_id = get_product_id(cursor, item[0])
    transaction_id = get_transaction_id(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent)
    check_sql = """
        SELECT 1 FROM Orders 
        WHERE transaction_id = %s AND product_id = %s
    """
    cursor.execute(check_sql, (transaction_id, product_id))
    if cursor.fetchone() is not None:
        return None  # Exit the function 

    # Insert the order 
    insert_sql = """
        INSERT INTO Orders (transaction_id, product_id) 
        VALUES (%s, %s);
        SELECT order_id FROM Orders ORDER BY order_id DESC;
    """
    print(f"Transaction ID: {transaction_id}, Product ID: {product_id}")
    cursor.execute(insert_sql, (transaction_id, product_id))
    order_id = cursor.fetchone()[0]
    return order_id

# Function to process and insert orders
def process_orders(cursor, transformed_data):
    order_ids = []
    for data_dict in transformed_data:
        try:
            # extracting transaction  from the data dictionary
            transaction_date = data_dict['transaction_date']
            transaction_time = data_dict['transaction_time']
            location_name = data_dict['location']
            payment_method = data_dict['payment_method']
            total_spent = float(data_dict['total_spent'])


            for item in data_dict['items']:
                insert_order(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent, item)


        except KeyError as e:
            print(f"Missing key {str(e)}, skipping entry: {data_dict}")
            continue  

        except Exception as e:
            print(f"Error inserting order: {str(e)}")
            continue

    cursor.connection.commit()
    return order_ids

# Get the SSM Param from AWS and turn it into JSON
# Don't log the password!
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

# Use the redshift details json to connect
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