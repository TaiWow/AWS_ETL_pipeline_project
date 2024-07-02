import csv_transform
import db_connection 


def insert_order(cursor, transaction_id, product_id):
    
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
            # Extract transaction_id and product_ids from the data dictionary
            transaction_id = data_dict['transaction_id']
            products = data_dict.get('products', [])  

            if not products:
                print(f"No products found for transaction_id {transaction_id}, skipping entry.")
                continue

            for product_id in products:
                order_id = insert_order(cursor, transaction_id, product_id)
                if order_id is not None:
                    order_ids.append(order_id)

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
