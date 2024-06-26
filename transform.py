import csv

# Extract

data_list = []
order = ['date_time', 'location', 'customer_name', 'items', 'total_amount', 'payment_method', 'card_number']

# Reads from a CSV file to create a list of dictionaries, takes fieldnames as an argument
def csv_to_list(list, path, fieldnames):
    with open(path, 'r') as file:
        csv_file = csv.DictReader(file, fieldnames=fieldnames)
        for row in csv_file:
            list.append(row)
    return list

def transform_data(list_of_dicts):
    # Initialize an empty list for transformed data
    transformed_data = []
    for data_dict in list_of_dicts:
        # Extract the transaction time, location, and payment method from list_of_dicts
        date_time = data_dict['date_time']
        transaction_date, transaction_time = date_time.split(' ', 1)
        location = data_dict['location']
        payment_method = data_dict['payment_method']
        
        # Split the product items strings into individual lists
        items = data_dict['items'].split(',')
        for item in items:
            # Split the items into product and price
            product_name, product_price = item.rsplit(' - ', 1)
          
            product_name = product_name.strip()  # Remove trailing spaces
          
            product_price = float(product_price.strip())  # Strip spaces and convert string to float
            
            # Create new dictionary and append new dictionary with transformed data
            transformed_data.append({
                'transaction_date': transaction_date,
                'transaction_time': transaction_time,
                'location': location,
                'product_name': product_name,
                'product_price': product_price,
                'payment_method': payment_method
            })
    return transformed_data

if __name__ == '__main__':
    data_list = csv_to_list(data_list, 'leeds.csv', order)
    transformed_data = transform_data(data_list)
    
    for entry in transformed_data:
        print(f"Transaction Date: {entry['transaction_date']}")
        print(f"Transaction Time: {entry['transaction_time']}")
        print(f"Location: {entry['location']}")
        print(f"Product: {entry['product_name']}, Price: {entry['product_price']}")
        print(f"Payment Method: {entry['payment_method']}")
        print("-" * 30)

