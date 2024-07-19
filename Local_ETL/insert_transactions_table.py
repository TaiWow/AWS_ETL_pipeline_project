import Local_ETL.csv_transform as csv_transform   
import Local_ETL.db_connection as db_connection 
from Local_ETL.insert_location_table import insert_location



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
            VALUES (%s, %s, %s, %s,%s)
            RETURNING transaction_id;
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

                process_transactions(cursor, transformed_data)
                
            connection.commit()
            print("transcanction load executed successfully.")
            
        except Exception as e:
            connection.rollback()
            print(f"Error: {e}")
            
        finally:
            cursor.close()
            connection.close()

    



  

    



  
