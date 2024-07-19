import os
import boto3
import json
import psycopg2 as psy

s3 = boto3.client('s3')

ssm_client = boto3.client('ssm', region_name='eu-west-1') #added region due to NoRegionError

ssm_env_var_name = 'SSM_PARAMETER_NAME'
sqs = boto3.client('sqs')
queue_url = 'https://sqs.eu-west-1.amazonaws.com/992382716453/nubi-queue'

def lambda_handler(event, context):
    for record in event['Records']:
        cur = None
        conn = None
        try:
            # Process SQS message
            transformed_data = json.loads(record['body'])
            
            # connection
            nubi_redshift_settings = os.environ['SSM_PARAMETER_NAME']
            redshift_details = get_ssm_param(nubi_redshift_settings)
            
            conn, cur = open_sql_database_connection_and_cursor(redshift_details)
            
            # load data
            process_products_list(cur, transformed_data)
            process_locations(cur, transformed_data)
            process_transactions(cur, transformed_data)
            process_orders(cur, transformed_data)
            
            # Commit changes to database
            conn.commit()
            
            # Delete SQS message
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=record['receiptHandle']
            )
            
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

def process_products_list(cursor, transformed_data):
    for data_dict in transformed_data:
        product_name = data_dict['product_name']
        product_price = data_dict['product_price']
        insert_product(cursor, product_name, product_price)
    
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

def process_locations(cursor, transformed_data):
    for data_dict in transformed_data:
        location_name = data_dict['location']
        insert_location(cursor, location_name)
    
    cursor.connection.commit()

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

def process_transactions(cursor, transformed_data):
    for data_dict in transformed_data:
        try:
            transaction_date = data_dict['transaction_date']
            transaction_time = data_dict['transaction_time']
            location_name = data_dict['location']
            payment_method = data_dict['payment_method']
            total_spent = float(data_dict['total_spent'])
        
            insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent)
            cursor.connection.commit()
        
        except KeyError as e:
            print(f"Missing transaction data, skipping entry: {str(e)}")
            continue  # Skip the current transaction
        
        except Exception as e:
            print(f"Error inserting transaction: {str(e)}")
            continue  # Skip the current transaction

def insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent):
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
            continue  # Skip the current order
        
        except Exception as e:
            print(f"Error inserting order: {str(e)}")
            continue  # Skip the current order

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
