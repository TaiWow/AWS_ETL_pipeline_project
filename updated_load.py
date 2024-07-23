


def fetch_location_id(cursor, location_name):
    cursor.execute("SELECT location_id FROM Location WHERE location_name = %s", (location_name,))
    result = cursor.fetchone()
    return result[0] if result else None


def fetch_product_id(cursor, product_name, product_price):
    cursor.execute("SELECT product_id FROM Products WHERE product_name = %s AND product_price = %s", (product_name, product_price))
    result = cursor.fetchone()
    return result[0] if result else None

def fetch_transaction_id(cursor, transaction_date, transaction_time, location_id):
    cursor.execute("""
        SELECT transaction_id 
        FROM Transactions 
        WHERE transaction_date = %s 
        AND transaction_time = %s 
        AND location_id = %s
        """, (transaction_date, transaction_time, location_id))
    result = cursor.fetchone()
    return result[0] if result else None

def insert_locations(cursor, transformed_data):
    locations_to_insert = []
    for data_dict in transformed_data:
        location_name = data_dict['location']
        if location_name:
            cursor.execute("SELECT location_id FROM Location WHERE location_name = %s", (location_name,))
            if cursor.fetchone() is None: 
                locations_to_insert.append((location_name,))
    
    if locations_to_insert:
        cursor.executemany("INSERT INTO Location (location_name) VALUES (%s)", locations_to_insert)
        print("insert_location completed sucessfully")
        
def insert_products(cursor, transformed_data):
    products_to_insert = []
    for data_dict in transformed_data:
        product_name = data_dict['product_name']
        product_price = data_dict['product_price']
        cursor.execute("SELECT product_id FROM Products WHERE product_name = %s AND product_price = %s", (product_name, product_price))
        if cursor.fetchone() is None:  
            products_to_insert.append((product_name, product_price))
    
    if products_to_insert:
        cursor.executemany("INSERT INTO Products (product_name, product_price) VALUES (%s, %s)", products_to_insert)
        print("insert_products completed sucessfully")

def insert_transactions(cursor, transformed_data):
    transactions_to_insert = []
    for data_dict in transformed_data:
        location_name = data_dict['location']
        transaction_date = data_dict['transaction_date']
        transaction_time = data_dict['transaction_time']
        payment_method = data_dict['payment_method']
        total_spent = data_dict['total_spent']
        
        location_id = fetch_location_id(cursor, location_name)
        if location_id:
            transaction_id = fetch_transaction_id(cursor, transaction_date, transaction_time, location_id)
            if transaction_id is None:  
                transactions_to_insert.append((transaction_date, transaction_time, location_id, payment_method, total_spent))
    
    if transactions_to_insert:
        cursor.executemany("INSERT INTO Transactions (transaction_date, transaction_time, location_id, payment_method, total_spent) VALUES (%s, %s, %s, %s, %s)", transactions_to_insert)
        print("insert_transanction completed sucessfully")

def insert_orders(cursor, transformed_data):
    orders_to_insert = []
    for data_dict in transformed_data:
        product_name = data_dict['product_name']
        product_price = data_dict['product_price']
        transaction_date = data_dict['transaction_date']
        transaction_time = data_dict['transaction_time']
        quantity = data_dict['quantity']
        
        product_id = fetch_product_id(cursor, product_name, product_price)
        location_id = fetch_location_id(cursor, data_dict['location'])
        transaction_id = fetch_transaction_id(cursor, transaction_date, transaction_time, location_id)
        
        if product_id and transaction_id:
            cursor.execute("SELECT order_id FROM Orders WHERE transaction_id = %s AND product_id = %s", (transaction_id, product_id))
            if cursor.fetchone() is None:  
                orders_to_insert.append((transaction_id, product_id, quantity))
    
    if orders_to_insert:
        cursor.executemany("INSERT INTO Orders (transaction_id, product_id, quantity) VALUES (%s, %s, %s)", orders_to_insert)
        print("insert_order completed sucessfully")

