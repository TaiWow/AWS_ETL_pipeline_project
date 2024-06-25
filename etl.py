import csv

#Extract

data_list = []
order = 'datetime', 'location', 'name', 'products', 'total_spending', 'CASH/CARD', 'card_number'

#Reads from a csv file to create a list of dictionaries, takes fieldnames as a arguement
def csv_to_list(list, path, fieldnames):
    with open(path, 'r') as file:
        csv_file = csv.DictReader(file, fieldnames=fieldnames)
        for row in csv_file:
            list.append(row) 
    return list

leeds_data = csv_to_list(data_list, 'leeds.csv', order)
for dict in leeds_data:
    print(dict)