import Local_ETL.csv_transform as csv_transform   
import Local_ETL.db_connection as db_connection 



def insert_product(cursor, product_name, product_price):
    
        cursor.execute("SELECT product_id FROM Products WHERE product_name = %s AND product_price = %s", (product_name, product_price))
        existing_product = cursor.fetchone()
        
        if existing_product:
            return existing_product[0]
        
        cursor.execute("""
            INSERT INTO Products (product_name, product_price) VALUES (%s, %s)
            RETURNING product_id;
        """, (product_name, product_price))
        
        product_id = cursor.fetchone()[0]
        return product_id
    
def process_products_list(cursor, transformed_data):
    for data_dict in transformed_data:
        product_name = data_dict['product_name']
        product_price = data_dict['product_price']
        insert_product(cursor, product_name, product_price)
    
    cursor.connection.commit()



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

                process_products_list(cursor, transformed_data)
                
            connection.commit()
            print("Products updated and executed successfully.")
            
        except Exception as e:
            connection.rollback()
            print(f"Error: {e}")
            
        finally:
            cursor.close()
            connection.close()





