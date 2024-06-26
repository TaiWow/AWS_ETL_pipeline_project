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
port_number =int(os.getenv("POSTGRES_PORT", 5432))


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

def drop_table_if_exists(connection):
    drop_table_query = "DROP TABLE IF EXISTS transactions;"
    try:
        with connection.cursor() as cursor:
            cursor.execute(drop_table_query)
        connection.commit()
        print("Existing table dropped successfully (if it existed).")
        print_separator()
    except psycopg2.Error as e:
        print(f"Error dropping table: {e}")
        print_separator()


def create_db_tables(connection):
    try:
        with connection.cursor() as cursor:
            # Read SQL file
            sql_file = open('nubi_setup.sql', 'r')
            sql_commands = sql_file.read()

            # Execute each command in the SQL file
            cursor.execute(sql_commands)
            connection.commit()
            print("Tables created successfully.")

    except Exception as e:
        print(f"Error creating tables: {e}")


# def create_db_tables(connection):
#     create_transaction_table = """
#         CREATE TABLE IF NOT EXISTS transactions (
#             id SERIAL PRIMARY KEY,
#             transaction_date DATE,
#             transaction_time TIME,
#             location VARCHAR(50),
#             product_name VARCHAR(100),
#             product_price FLOAT,
#             payment_method VARCHAR(20)
#         );
#     """
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute(create_transaction_table)
#         connection.commit()
#         print("Table created successfully.")
#         print_separator()
#     except psycopg2.Error as e:
#         print(f"Error creating table: {e}")
#         print_separator()

def insert_transactions(connection, data_list):
    sql = """
        INSERT INTO transactions (transaction_date, transaction_time, location, product_name, product_price, payment_method)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    try:
        with connection.cursor() as cursor:
            for transaction in data_list:
                cursor.execute(sql, (
                    transaction['transaction_date'],
                    transaction['transaction_time'],
                    transaction['location'],
                    transaction['product_name'],
                    transaction['product_price'],
                    transaction['payment_method']
                ))
        connection.commit()
        print('Rows inserted successfully.')
        print_separator()
    except psycopg2.Error as e:
        print(f"Error inserting rows: {e}")
        print_separator()

def csv_to_list(filename):
    column_names = [
        'date_time',
        'location',
        'customer_name',
        'items',
        'total_amount',
        'payment_method',
        'card_number'
    ]
    try:
        with open(filename, newline='') as file:
            reader = csv.DictReader(file, fieldnames=column_names, delimiter=',')
            return list(reader)
    except FileNotFoundError as e:
        print(f"Error reading CSV file: {e}")
        print_separator()
        return []

def transform_data(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        try:
             #Extract the transaction time, location, and payment method from list_of_dicts
            date_time = data_dict['date_time']
            transaction_date, transaction_time = date_time.split(' ', 1)
            location = data_dict['location']
            payment_method = data_dict['payment_method']
        
        # Split the product items strings into individual lists
            items = data_dict['items'].split(',')
            for item in items:
            # Split the items into product and price
                product_name, product_price = item.rsplit(' - ', 1)
          
                product_name = product_name.strip()  # Remove trailing spaces
          
                product_price = float(product_price.strip())  # Strip spaces and convert string to float
            
            # Create new dictionary and append new dictionary with transformed data
                transformed_data.append({
                'transaction_date': transaction_date,
                'transaction_time': transaction_time,
                'location': location,
                'product_name': product_name,
                'product_price': product_price,
                'payment_method': payment_method
            })
        except ValueError as e:
            print(f"Error processing transformation: {data_dict}")
            print(f"Error: {e}")
            print_separator()
    return transformed_data

if __name__ == '__main__':
    connection = setup_db_connection()

    if connection:
        drop_table_if_exists(connection)
        create_db_tables(connection)

        data_list = csv_to_list('leeds.csv')
        if data_list:
            transformed_data_list = transform_data(data_list)
            if transformed_data_list:
                insert_transactions(connection, transformed_data_list)

        connection.close()
        print("Database connection closed.")
        print_separator()
