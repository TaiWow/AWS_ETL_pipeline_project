import csv

# Extract

data_list = []
order = ['transaction_time', 'location', 'customer_name', 'items', 'total_amount', 'payment_method', 'card_number']

# Reads from a csv file to create a list of dictionaries, takes fieldnames as an argument
def csv_to_list(list, path, fieldnames):
    with open(path, 'r') as file:
        csv_file = csv.DictReader(file, fieldnames=fieldnames)
        for row in csv_file:
            list.append(row)
    return list

def transform_data(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        transaction_time = data_dict['transaction_time']
        location = data_dict['location']
        payment_method = data_dict['payment_method']
        
        items = data_dict['items'].split(',')
        for item in items:
            product_name, product_price = item.rsplit('-', 1)
            product_name = product_name.strip()
            product_price = float(product_price.strip())
            
            transformed_data.append({
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
        print(f"Transaction Time: {entry['transaction_time']}")
        print(f"Location: {entry['location']}")
        print(f"Product: {entry['product_name']}, Price: {entry['product_price']}")
        print(f"Payment Method: {entry['payment_method']}")
        print("-" * 30)

