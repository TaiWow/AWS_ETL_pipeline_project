
import csv_transform
import db_connection


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
            location_list.append(location_name)  # Adds the location to the list of processed locations
    
    cursor.connection.commit()  




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
            
            # transformed data to insert unique locations into the database
            process_locations(cursor, transformed_data)
            
        connection.commit() 
            
        cursor.close()  
        connection.close()  

    