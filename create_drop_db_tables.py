import db_connection
import psycopg2



def create_db_tables(connection):
    try:
        with connection.cursor() as cursor:
            # Read SQL file
            sql_file = open('nubi_setup.sql', 'r')
            sql_commands = sql_file.read()

            # Execute each command in the SQL file
            cursor.execute(sql_commands)
            connection.commit()
            print("Tables created successfully.")

    except Exception as e:
        print(f"Error creating tables: {e}")



       
  
    
if __name__ == '__main__':
    connection = db_connection.setup_db_connection()
   
    
    if connection:
        create_db_tables(connection)
    
             

              
    
        
        

  

      
     




      

