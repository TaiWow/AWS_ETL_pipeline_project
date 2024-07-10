import csv_transform
import db_connection
from insert_location_table import process_locations
from insert_product_table import process_products_list
from insert_transactions_table import process_transactions
from insert_orders_table import process_orders
from create_drop_db_tables import create_db_tables

def perform_etl():
    # Set up the database connection
    connection = db_connection.setup_db_connection()

    if connection:
        create_db_tables(connection)

        cursor = connection.cursor()

        # Load and transform CSV data
        leeds_data = csv_transform.csv_to_list('leeds.csv')
        chesterfield_data = csv_transform.csv_to_list('chesterfield_25-08-2021_09-00-00.csv')

        combined_data = leeds_data + chesterfield_data

        if combined_data:
            # Applied transformation steps
            transformed_data = csv_transform.remove_sensitive_data(combined_data)
            transformed_data = csv_transform.split_date_and_time(transformed_data)
            transformed_data = csv_transform.split_items_and_count_quantity(transformed_data)

            # Process and insert the transformed data into the database
            process_products_list(cursor, transformed_data)
            process_locations(cursor, transformed_data)
            process_transactions(cursor, transformed_data)
            process_orders(cursor, transformed_data)

        connection.commit()

        cursor.close()
        connection.close()

        print('ETL process completed successfully')

if __name__ == '__main__':
    perform_etl()