import csv_transform
import db_connection 
from insert_product_table import insert_product
from insert_transactions_table import insert_transaction


def insert_order(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent, item):
    
    product_id = insert_product(cursor, item[0], item[1])
    transaction_id = insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent)
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
        VALUES (%s, %s)
        RETURNING order_id;
    """
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

if __name__ == '__main__':
    # Establish a database connection
    connection = db_connection.setup_db_connection()

    if connection:
        cursor = connection.cursor()
        
    
        leeds_data = csv_transform.csv_to_list('leeds.csv')
        chesterfield_data = csv_transform.csv_to_list('chesterfield_25-08-2021_09-00-00.csv')
        combined_data = leeds_data + chesterfield_data  

        if combined_data:
            
            transformed_data = csv_transform.remove_sensitive_data(combined_data)
            transformed_data = csv_transform.split_date_and_time(transformed_data)
            transformed_data = csv_transform.split_items_into_list(transformed_data)

            
            order_ids = process_orders(cursor, transformed_data)
            print(f"Inserted order IDs: {order_ids}")

        cursor.close()  
        connection.close()
