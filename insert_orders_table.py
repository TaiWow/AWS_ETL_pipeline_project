import csv_transform
import db_connection
import insert_location_table

import insert_transactions_table



def insert_order(cursor, transaction_id, location_id, product_id, total_spent, payment_method):
    order_sql = """
        INSERT INTO Orders (transaction_id, location_id, product_id, total_spent, payment_method)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING order_id;
    """
    cursor.execute(order_sql, (
        transaction_id,
        location_id,
        product_id,
        total_spent,
        payment_method
    ))
    order_id = cursor.fetchone()[0]
    return order_id

def process_orders(cursor, transformed_data):
    order_list = []

    for data_dict in transformed_data:
        transaction_id = data_dict['transaction_id']
        location_id = data_dict['location_id']
        products = data_dict['items']
        payment_method = data_dict['payment_method']
        total_spent = data_dict['total_spent']

        for product_name, product_price in products:
            # Assume product_id retrieval based on product_name
            product_id = retrieve_product_id(cursor, product_name)

            order_id = insert_order(cursor, transaction_id, location_id, product_id, product_price, payment_method)
            if order_id:
                order_list.append(order_id)
    
    cursor.connection.commit()
    return order_list


if __name__ == '__main__':
    connection = db_connection.setup_db_connection()

    if connection:
        cursor = connection.cursor()

        try:
            leeds_data = csv_transform.csv_to_list('leeds.csv')
            chesterfield_data = csv_transform.csv_to_list('chesterfield_25-08-2021_09-00-00.csv')
            combined_data = leeds_data + chesterfield_data

            if combined_data:
                transformed_data = csv_transform.remove_sensitive_data(combined_data)
                transformed_data = csv_transform.split_date_and_time(transformed_data)
                transformed_data = csv_transform.split_items_into_list(transformed_data)

                # Process transactions and get transaction_ids
                transaction_ids = insert_transactions_table.process_transactions(cursor, transformed_data)

                # Process orders with retrieved transaction_ids
                for data_dict, transaction_id in zip(transformed_data, transaction_ids):
                    data_dict['transaction_id'] = transaction_id
                    location_name = data_dict['location']
                    location_id = insert_location_table.retrieve_location_id(cursor, location_name)
                    data_dict['location_id'] = location_id

                order_ids = process_orders(cursor, transformed_data)
                print(f"Successfully processed {len(order_ids)} orders.")

            connection.commit()

        finally:
            cursor.close()
            connection.close()
