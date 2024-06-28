
from calendar import c
import csv_transform
import db_connection


def insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent):
   
    check_sql = """
        SELECT 1 FROM Transactions 
        WHERE transaction_date = %s AND transaction_time = %s AND location_name = %s AND payment_method = %s
    """
    cursor.execute(check_sql, (transaction_date, transaction_time, location_name, payment_method))
    if cursor.fetchone() is not None:
        return None

    insert_sql = """
        INSERT INTO Transactions (transaction_date, transaction_time, location_name, payment_method, total_spent) VALUES (%s, %s, %s, %s, %s)
        RETURNING transaction_id;
    """
    cursor.execute(insert_sql, (transaction_date, transaction_time, location_name, payment_method, total_spent))
   
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
            if transaction_id is not None:
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


def insert_product(cursor, product_name, product_price):
    
    # checks if product name already exists in the database
    cursor.execute("SELECT 1 FROM products WHERE product_name = %s", (product_name,))
    if cursor.fetchone() is not None:
        print(f"Product '{product_name}' already exists. Skipping......")
        print("---"*30)
        return  # exit the function without inserting if the product  exists

    product_sql = """
        INSERT INTO Products (product_name, product_price) VALUES (%s, %s)
        RETURNING product_id;
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


def insert_location(cursor, location_name):
    # checks if location name already exists in the database
    cursor.execute("SELECT 1 FROM location WHERE location_name = %s", (location_name,))
    if cursor.fetchone() is not None:
        print(f"Location '{location_name}' already exists. Skipping......")
        return  # Exit the function without inserting

    # Insert the location if it's unique and get the generated location_id
    cursor.execute("""
        INSERT INTO location (location_name) VALUES (%s)
        RETURNING location_id;
    """, (location_name,))
    location_id = cursor.fetchone()[0] 
    return location_id

def process_locations(cursor, transformed_data):
    location_list = []  # an empty list to keep track of processed locations
    
    for data_dict in transformed_data:
        location_name = data_dict['location']
        if location_name not in location_list:
            insert_location(cursor, location_name)  # Insert the location if it's not already processed
            location_list.append(location_name)  # Add the location to the list of processed locations
    
    cursor.connection.commit()  


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

def process_orders(cursor, transformed_data):
    order_list = []

    for data_dict in transformed_data:
        transaction_id = data_dict['transaction_id']
        location_id = data_dict['location_id']
        products = data_dict['items']
        payment_method = data_dict['payment_method']
        total_spent = data_dict['total_spent']

        for product_name, product_price in products:
            # Assume product_id retrieval based on product_name
            product_id = retrieve_product_id(cursor, product_name)

            order_id = insert_order(cursor, transaction_id, location_id, product_id, product_price, payment_method)
            if order_id:
                order_list.append(order_id)
    
    cursor.connection.commit()
    return order_list


def process_orders(cursor, transformed_data):
    order_ids = []
    for data_dict in transformed_data:
        transaction_id = data_dict['transaction_id']
        location_id = data_dict['location_id']
        products = data_dict['items']
        payment_method = data_dict['payment_method']
        total_spent = data_dict['total_spent']

        for product_name, product_price in products:
            try:
                # Retrieve product_id based on product_name
                product_id = retrieve_product_id(cursor, product_name)

                # Insert the order
                order_id = insert_order(cursor, transaction_id, location_id, product_id, product_price, payment_method)
                if order_id:
                    order_ids.append(order_id)

            except Exception as e:
                print(f"Error processing order for product '{product_name}': {str(e)}")
                continue  # Skip to the next product

    cursor.connection.commit()
    return order_ids

def retrieve_product_id(cursor, product_name):
    query = "SELECT product_id FROM Products WHERE product_name = %s;"
    cursor.execute(query, (product_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError(f"Product '{product_name}' not found in database.")
    
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

            process_products_list(cursor, transformed_data)
            process_locations(cursor, transformed_data)
            transaction_ids = process_transactions(cursor, transformed_data)
            order_ids = process_orders(cursor, transformed_data)

            connection.commit()

        except Exception as e:
            print(f"Error during main execution: {str(e)}")
            connection.rollback()

        finally:
            cursor.close()
            connection.close()

    
    
  


