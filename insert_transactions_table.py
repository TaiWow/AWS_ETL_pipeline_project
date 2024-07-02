
import csv_transform
import db_connection
from insert_location_table import insert_location
import insert_location_table

#insert_transctiobns._table.py


def insert_transaction(cursor, transaction_date, transaction_time, location_name, payment_method, total_spent):
    
    location_id = insert_location_table.insert_location(cursor, location_name)
    check_sql = """
        SELECT 1 FROM Transactions 
        WHERE transaction_date = %s AND transaction_time = %s AND location_id = %s AND payment_method = %s
    """
    cursor.execute(check_sql, (transaction_date, transaction_time, location_id, payment_method))
    if cursor.fetchone() is not None:
        return None

    insert_sql = """
        INSERT INTO Transactions (transaction_date, transaction_time, location_id, payment_method, total_spent) VALUES (%s, %s, %s, %s, %s)
        RETURNING transaction_id;
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

# Main block to execute the script
if __name__ == '__main__':
   
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
            
            
            
            transaction_ids = process_transactions(cursor, transformed_data)
            print(f"Successfully processed {len(transaction_ids)} transactions.")

            
            
            connection.commit()

            cursor.close()
            connection.close()
