
from etl_shared.transform import (
    csv_text_to_list,
    remove_sensitive_data as shared_remove_sensitive_data,
    split_date_and_time as shared_split_date_and_time,
    split_items_and_count_quantity as shared_split_items_and_count_quantity,
)



def csv_to_list(path):
    with open(path, 'r') as file:
        return csv_text_to_list(file.read())

def remove_sensitive_data(list_of_dicts):
    return shared_remove_sensitive_data(list_of_dicts)



def split_date_and_time(list_of_dicts):
    return shared_split_date_and_time(list_of_dicts)


def split_items_and_count_quantity(list_of_dicts):
    return shared_split_items_and_count_quantity(list_of_dicts)

def print_transformed_data(transformed_data):
    for entry in transformed_data:
        print(f"Transaction Date: {entry['transaction_date']}")
        print(f"Transaction Time: {entry['transaction_time']}")
        print(f"Location: {entry['location']}")
        print(f"Product Name: {entry['product_name']}")
        print(f"Product Price: {entry['product_price']}")
        print(f"Quantity: {entry['quantity']}")
        print(f"Total Spent: {entry['total_spent']}")
        print(f"Payment Method: {entry['payment_method']}")
        print("-" * 30)

 
    
if __name__ == '__main__':
    leeds_data = csv_to_list('leeds.csv')
    chesterfield_data = csv_to_list('chesterfield_25-08-2021_09-00-00.csv')

    combined_data = leeds_data + chesterfield_data

    transformed_data = remove_sensitive_data(combined_data)
    transformed_data = split_date_and_time(transformed_data)
    transformed_data = split_items_and_count_quantity(transformed_data)

 
    print_transformed_data(transformed_data)
    
