#
import csv_transform  
import db_connection  

#insert_products_table.py

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



if __name__ == '__main__':
    # Set up the database connection
    connection = db_connection.setup_db_connection()

    if connection:
        
        cursor = connection.cursor()
        
        # load and transform CSV data
        leeds_data = csv_transform.csv_to_list('leeds.csv')
        chesterfield_data = csv_transform.csv_to_list('chesterfield_25-08-2021_09-00-00.csv')
       
        combined_data = leeds_data + chesterfield_data
        
        if combined_data:
            # applied transformation steps 
            transformed_data = csv_transform.remove_sensitive_data(combined_data)
            transformed_data = csv_transform.split_date_and_time(transformed_data)
            transformed_data = csv_transform.split_items_into_list(transformed_data)
            
            # Process and insert the transformed data into the database
            process_products_list(cursor, transformed_data)
          
           
    
    connection.commit()
            
   
    cursor.close()
    connection.close()
