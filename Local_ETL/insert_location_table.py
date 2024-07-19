import  Local_ETL.csv_transform as csv_transform
import  Local_ETL.db_connection as db_connection



def insert_location(cursor, location_name):
    cursor.execute("SELECT location_id FROM Location WHERE location_name = %s", (location_name,))
    existing_location = cursor.fetchone()
    
    if existing_location:
        return existing_location[0]
        
    cursor.execute("""
        INSERT INTO location (location_name) VALUES (%s)
        RETURNING location_id;
    """, (location_name,))
    
    location_id = cursor.fetchone()[0]
    return location_id

def process_locations(cursor, transformed_data):
    for data_dict in transformed_data:
        location_name = data_dict['location']
        insert_location(cursor, location_name)
    
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

                process_locations(cursor, transformed_data)
                
            connection.commit()
            print("Locations updated and executed successfully.")
            
        except Exception as e:
            connection.rollback()
            print(f"Error: {e}")
            
        finally:
            cursor.close()
            connection.close()

    