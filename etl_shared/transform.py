import csv
from collections import Counter
from datetime import datetime

DEFAULT_COLUMN_NAMES = [
    'date_time',
    'location',
    'customer_name',
    'items',
    'total_spent',
    'payment_method',
    'card_number',
]


def csv_text_to_list(csv_text, column_names=None):
    data_list = []
    names = column_names or DEFAULT_COLUMN_NAMES
    csv_reader = csv.DictReader(csv_text.splitlines(), fieldnames=names)
    for row in csv_reader:
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
            'payment_method': data_dict['payment_method'],
        })
    return transformed_data


def split_date_and_time(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        date_time = data_dict['date_time']
        transaction_date, transaction_time = date_time.split(' ', 1)
        transaction_date = datetime.strptime(transaction_date, '%d/%m/%Y').strftime('%Y-%m-%d')

        transformed_data.append({
            'date_time': date_time,
            'transaction_date': transaction_date,
            'transaction_time': transaction_time,
            'location': data_dict['location'],
            'items': data_dict['items'],
            'total_spent': data_dict['total_spent'],
            'payment_method': data_dict['payment_method'],
        })
    return transformed_data


def split_items_and_count_quantity(list_of_dicts):
    transformed_data = []
    for data_dict in list_of_dicts:
        items = data_dict['items'].split(',')
        item_counts = Counter()
        item_prices = {}

        for item in items:
            product_name, product_price = item.rsplit(' - ', 1)
            product_name = product_name.strip()
            product_price = float(product_price.strip())
            item_counts[product_name] += 1
            item_prices[product_name] = product_price

        for product_name, quantity in item_counts.items():
            transformed_data.append({
                'transaction_date': data_dict['transaction_date'],
                'transaction_time': data_dict['transaction_time'],
                'location': data_dict['location'],
                'product_name': product_name,
                'product_price': item_prices[product_name],
                'quantity': quantity,
                'total_spent': float(data_dict['total_spent']),
                'payment_method': data_dict['payment_method'],
            })
    return transformed_data


def transform_data(data_list):
    transformed_data = remove_sensitive_data(data_list)
    transformed_data = split_date_and_time(transformed_data)
    transformed_data = split_items_and_count_quantity(transformed_data)
    return transformed_data
