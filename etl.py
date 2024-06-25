import csv

#Reads from a csv file to create a list of dictionaries
def csv_to_list(list, path):
    with open(path, 'r') as file:
        csv_file = csv.DictReader(file)
        for row in csv_file:
            list.append(row) 
    return list 