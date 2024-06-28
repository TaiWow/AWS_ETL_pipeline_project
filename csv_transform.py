
import csv
from datetime import datetime



def csv_to_list(path):
    data_list = []
    column_names = ['date_time', 'location', 'customer_name', 'items', 'total_spent', 'payment_method', 'card_number']

    with open(path, 'r') as file:
        csv_file = csv.DictReader(file, fieldnames=column_names)
        for row in csv_file:
            data_list.append(row)
    return data_list

def remove_sensitive_data(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        transformed_data.append({
            'date_time': data_dict['date_time'],
            'location': data_dict['location'],
            'items': data_dict['items'],
            'total_spent': data_dict['total_spent'],
            'payment_method': data_dict['payment_method']
        })
    return transformed_data



def split_date_and_time(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        date_time = data_dict['date_time']
        transaction_date, transaction_time = date_time.split(' ', 1)
        
        # Convert transaction_date to YYYY-MM-DD format
        transaction_date = datetime.strptime(transaction_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        
        transformed_data.append({
            'date_time': date_time,  # Keep original for reference
            'transaction_date': transaction_date,
            'transaction_time': transaction_time,
            'location': data_dict['location'],
            'items': data_dict['items'],
            'total_spent': data_dict['total_spent'],  # Corrected key name here
            'payment_method': data_dict['payment_method']
        })
    return transformed_data


def split_items_into_list(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        items = data_dict['items'].split(',')
        item_list = []
        for item in items:
            product_name, product_price = item.rsplit(' - ', 1)
            product_name = product_name.strip()
            product_price = float(product_price.strip())
            item_list.append((product_name, product_price))
        transformed_data.append({
            'transaction_date': data_dict['transaction_date'],  # Use transformed fields from previous step
            'transaction_time': data_dict['transaction_time'],
            'location': data_dict['location'],
            'items': item_list,
            'total_spent': data_dict['total_spent'],
            'payment_method': data_dict['payment_method']
        })
    return transformed_data

def print_transformed_data(transformed_data):
    for entry in transformed_data:
        print(f"Transaction Date: {entry['transaction_date']}")
        print(f"Transaction Time: {entry['transaction_time']}")
        print(f"Location: {entry['location']}")
        for product_name, product_price in entry['items']:
            print(f"Products: {product_name}, {product_price}")
        print(f"Payment Method: {entry['payment_method']}")
        print(f"Total spent {entry['total_spent']}")
        print("-" * 30)

        
    
if __name__ == '__main__':
    leeds_data = csv_to_list('leeds.csv')
    chesterfield_data = csv_to_list('chesterfield_25-08-2021_09-00-00.csv')

    combined_data = leeds_data + chesterfield_data

    transformed_data = remove_sensitive_data(combined_data)
    transformed_data = split_date_and_time(transformed_data)
    transformed_data = split_items_into_list(transformed_data)

 
    print_transformed_data(transformed_data)
    