import Local_ETL.csv_transform as csv_transform   
import Local_ETL.db_connection as db_connection 
from Local_ETL.insert_product_table import insert_product
from Local_ETL.insert_transactions_table import insert_transaction  



def insert_order(cursor, quantity, product_name, product_price, transaction_date, transaction_time, location_name, payment_method, total_spent):
    
    transaction_id = insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent)
    
    product_id = insert_product(cursor, product_name, product_price)
        
    cursor.execute("SELECT order_id FROM Orders WHERE transaction_id = %s AND product_id = %s", (transaction_id, product_id))
    existing_order = cursor.fetchone()
        
    if existing_order:
        return existing_order[0]
        
    cursor.execute("""
        INSERT INTO Orders (transaction_id, product_id, quantity) 
        VALUES (%s, %s, %s) 
        RETURNING order_id;
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



if __name__ == '__main__':
    connection = db_connection.setup_db_connection()

    if connection:
        try:
            cursor = connection.cursor() 
            
            leeds_data = csv_transform.csv_to_list('leeds.csv')
            chesterfield_data = csv_transform.csv_to_list('chesterfield_25-08-2021_09-00-00.csv')
            combined_data = leeds_data + chesterfield_data  
            
            if combined_data:
                transformed_data = csv_transform.remove_sensitive_data(combined_data)
                transformed_data = csv_transform.split_date_and_time(transformed_data)
                transformed_data = csv_transform.split_items_and_count_quantity(transformed_data)

                process_orders(cursor, transformed_data)
                
            connection.commit()
            print("Orders updated and executed successfully.")
            
        except Exception as e:
            connection.rollback()
            print(f"Error: {e}")
            
        finally:
            cursor.close()
            connection.close()
