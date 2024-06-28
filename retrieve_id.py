



def retrieve_location_id(cursor, location_name):
    query = "SELECT location_id FROM Location WHERE location_name = %s;"
    cursor.execute(query, (location_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError(f"Location '{location_name}' not found in database.")

def retrieve_product_id(cursor, product_name):
    query = "SELECT product_id FROM Products WHERE product_name = %s;"
    cursor.execute(query, (product_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError(f"Product '{product_name}' not found in database.")

def retrieve_product_id(cursor, product_name):
    
    def retrieve_product_id(cursor, product_name):
    # Example implementation, replace with your actual logic to retrieve product_id
    query = "SELECT product_id FROM Products WHERE product_name = %s;"
    cursor.execute(query, (product_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError(f"Product '{product_name}' not found in database.")
    # Example implementation, replace with your actual logic to retrieve product_id
    query = "SELECT product_id FROM Products WHERE product_name = %s;"
    cursor.execute(query, (product_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError(f"Product '{product_name}' not found in database.")
