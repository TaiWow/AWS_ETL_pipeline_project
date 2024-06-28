import os
import csv
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details
host_name = os.getenv("POSTGRES_HOST", "localhost")
user_name = os.getenv("POSTGRES_USER", "yourusername")
user_password = os.getenv("POSTGRES_PASSWORD", "yourpassword")
database_name = os.getenv("POSTGRES_DB", "nubi_project_db")
port_number = int(os.getenv("POSTGRES_PORT", 5432))


def print_separator():
    print("------------")


def setup_db_connection():
    try:
        connection = psycopg2.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            dbname=database_name,
            port=port_number
        )
        print("Database connection successful.")
        print_separator()
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        print_separator()
        return None


# Extract 

def csv_to_list(path):
    data_list = []
    column_names  = ['date_time', 'location', 'customer_name', 'items', 'total_amount', 'payment_method', 'card_number']

    with open(path, 'r') as file:
        csv_file = csv.DictReader(file, fieldnames=column_names)
        for row in csv_file:
            data_list.append(row)
    return data_list

 # Transform 
 
def remove_sensitive_data(list_of_dicts):
    # Initialize an empty list for transformed data
    transformed_data = []
    for data_dict in list_of_dicts:

        # Create new dictionary and append new dictionary without the sensitive data like name and card details
        transformed_data.append({
            'date_time': data_dict['date_time'],
            'location': data_dict['location'],
            'items': data_dict['items'],
            'total_amount': data_dict['total_amount'],
            'payment_method': data_dict['payment_method']

        })
    
    return transformed_data

def split_date_and_time(list_of_dicts):
    # Initialize an empty list for transformed data
    transformed_data = []
    for data_dict in list_of_dicts:
        
        date_time = data_dict['date_time']
        transaction_date, transaction_time = date_time.split(' ', 1)

        # Create new dictionary and append new dictionary without the sensitive data like name and card details
        transformed_data.append({
            'transaction_date': transaction_date,
            'transaction_time': transaction_time,
            'location': data_dict['location'],
            'items': data_dict['items'],
            'total_amount': data_dict['total_amount'],
            'payment_method': data_dict['payment_method']

        })
    
    return transformed_data

def split_items_into_list(list_of_dicts):
    # Initialize an empty list for transformed data
    transformed_data = []
    for data_dict in list_of_dicts:
        # Split the product items strings into individual lists
        items = data_dict['items'].split(',')
        item_list  =[]
        for item in items:
            
            # Split the items into product and price
            product_name, product_price = item.rsplit(' - ', 1)
          
            product_name = product_name.strip()  # Remove trailing spaces
          
            product_price = float(product_price.strip())  # Strip spaces and convert string to float
            item_list.append((product_name,product_price))  

        # Create new dictionary and append new dictionary without the sensitive data like name and card details
        transformed_data.append({
            'transaction_date': data_dict['transaction_date'],
            'transaction_time': data_dict['transaction_time'],
            'location': data_dict['location'],
            'items': item_list,
            'total_amount': data_dict['total_amount'],
            'payment_method': data_dict['payment_method']

        }) 

    return transformed_data  

def print_transformed_data(transformed_data):
    for entry in transformed_data:
        print(f"Transaction Date: {entry['transaction_date']}")
        print(f"Transaction Time: {entry['transaction_time']}")
        print(f"Location: {entry['location']}")
        for product_name, product_price in entry['items']:
            print(f"Products: {product_name}, {product_price}")
        print(f"Payment Method: {entry['payment_method']}")
        print(f"Total Amount: {entry['total_amount']}")
        print("-" * 30)
        
# Load 


def insert_products(cursor,product_name, product_price):
    product_sql = """
        INSERT INTO products (product_name,product_price)
        VALUES (%s, %s)
        RETURNING product_id;
        """
    cursor.execute(product_sql, (product_name, product_price))
    product_id = cursor.fetchone()

    connection.commit() 
    # return product_id(0)

def insert_transaction(cursor, transaction_date, transaction_time, location_name, total_spent, payment_method):
    transaction_sql = """
        INSERT INTO transactions (transaction_date, transaction_time, location_name, total_spent, payment_method)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING transaction_id;
    """
    cursor.execute(transaction_sql, (transaction_date, transaction_time, location_name, total_spent, payment_method))
    transaction_id = cursor.fetchone()
    
    connection.commit()
    #return transaction_id

if __name__ == '__main__':
    connection = setup_db_connection()

    if connection:
        cursor = connection.cursor()
        
        data_list = csv_to_list('leeds.csv')
        if data_list:
            transformed_data = remove_sensitive_data(data_list)
            transformed_data = split_date_and_time(transformed_data)
            transformed_data = split_items_into_list(transformed_data)
            
            item_list = []
            for dict in transformed_data:
                for item in dict['items']:
                    if item not in item_list:
                        insert_products(cursor, item[0], item[1])
                        item_list.append(item)
                insert_transaction(
                    cursor,
                    dict['transaction_date'],
                    dict['transaction_time'],
                    dict['location'],
                    dict['total_amount'],
                    dict['payment_method']
                )
    
        cursor.close()
    
    
    

   
