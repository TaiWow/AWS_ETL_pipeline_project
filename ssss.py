
import csv
from datetime import datetime
import csv_transform
import db_connection
import insert_location_table
import insert_transactions_table
import insert_orders_table  # Assuming you have a script for inserting orders


def csv_to_list(path):
    data_list = []
    column_names = ['date_time', 'location', 'customer_name', 'items', 'total_spent', 'payment_method', 'card_number']

    with open(path, 'r') as file:
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

def print_transformed_data(transformed_data):
    for entry in transformed_data:
        print(f"Transaction Date: {entry['transaction_date']}")
        print(f"Transaction Time: {entry['transaction_time']}")
        print(f"Location: {entry['location']}")
        for product_name, product_price in entry['items']:
            print(f"Products: {product_name}, {product_price}")
        print(f"Payment Method: {entry['payment_method']}")
        print(f"Total spent {entry['total_spent']}")
        print("-" * 30)



def insert_order(cursor, transaction_id, location_id, product_id, total_spent, payment_method):
    order_sql = """
        INSERT INTO Orders (transaction_id, location_id, product_id, total_spent, payment_method)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING order_id;
    """
    cursor.execute(order_sql, (
        transaction_id,
        location_id,
        product_id,
        total_spent,
        payment_method
    ))
    order_id = cursor.fetchone()[0]
    return order_id

def retrieve_product_id(cursor, product_name):
    query = "SELECT product_id FROM Products WHERE product_name = %s;"
    cursor.execute(query, (product_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError(f"Product '{product_name}' not found in database.")

def process_orders(cursor, transformed_data):
    order_list = []

    for data_dict in transformed_data:
        transaction_id = data_dict['transaction_id']
        location_id = data_dict['location_id']
        products = data_dict['items']
        payment_method = data_dict['payment_method']
        total_spent = data_dict['total_spent']

        for product_name, product_price in products:
            product_id = retrieve_product_id(cursor, product_name)
            order_id = insert_order(cursor, transaction_id, location_id, product_id, product_price, payment_method)
            if order_id:
                order_list.append(order_id)
    
    cursor.connection.commit()
    return order_list

if __name__ == '__main__':
    connection = db_connection.setup_db_connection()

    if connection:
        cursor = connection.cursor()

        try:
            leeds_data = csv_transform.csv_to_list('leeds.csv')
            chesterfield_data = csv_transform.csv_to_list('chesterfield_25-08-2021_09-00-00.csv')
            combined_data = leeds_data + chesterfield_data

            if combined_data:
                transformed_data = csv_transform.remove_sensitive_data(combined_data)
                transformed_data = csv_transform.split_date_and_time(transformed_data)
                transformed_data = csv_transform.split_items_into_list(transformed_data)

                # Process transactions and get transaction_ids
                transaction_ids = insert_transactions_table.process_transactions(cursor, transformed_data)

                # Process orders with retrieved transaction_ids
                for data_dict, transaction_id in zip(transformed_data, transaction_ids):
                    data_dict['transaction_id'] = transaction_id
                    location_name = data_dict['location']
                    location_id = insert_location_table.retrieve_location_id(cursor, location_name)
                    data_dict['location_id'] = location_id

                order_ids = process_orders(cursor, transformed_data)
                print(f"Successfully processed {len(order_ids)} orders.")

            connection.commit()

        finally:
            cursor.close()
            connection.close()


