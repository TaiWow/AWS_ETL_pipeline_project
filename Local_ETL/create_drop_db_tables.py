import Local_ETL.db_connection as db_connection
import psycopg2



def create_db_tables(connection):
    try:
        with connection.cursor() as cursor:
            # Read SQL file
            sql_file = open('nubi_postgres_setup.sql', 'r')
            sql_commands = sql_file.read()

            # Execute each command in the SQL file
            cursor.execute(sql_commands)
            connection.commit()
            print("Tables created successfully.")

    except Exception as e:
        print(f"Error creating tables: {e}")
        


def delete_all_tables(connection):
    try:
        with connection.cursor() as cursor:
         
            drop_tables_sql = """
                DROP TABLE IF EXISTS Orders CASCADE;
                DROP TABLE IF EXISTS Transactions CASCADE;
                DROP TABLE IF EXISTS Products CASCADE;
                DROP TABLE IF EXISTS Location CASCADE;
            """
        
            cursor.execute(drop_tables_sql)
            connection.commit()
            
            print("All tables dropped successfully.")
    
    except psycopg2.Error as e:
        print(f"Error dropping tables: {e}")

if __name__ == '__main__':
    connection = db_connection.setup_db_connection()
    
    if connection:
        try:
           
            delete_all_tables(connection)
            create_db_tables(connection)
        
        
        finally:
            
            connection.close()
    
       
  

    
             

              
    
        
        

  

      
     




      

