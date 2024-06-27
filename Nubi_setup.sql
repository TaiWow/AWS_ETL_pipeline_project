-- Create the database if it does not exist (run this separately)
-- CREATE DATABASE Nubi_project;

-- Connect to the Nubi_project database
-- In Adminer, select the database from a dropdown then run the below code

-- Create the Products table
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_price DECIMAL(10,2) NOT NULL
);

-- Create the Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    transaction_time TIME NOT NULL,
    payment_method VARCHAR(100) NOT NULL,
    transaction_location VARCHAR(100) NOT NULL,
    total_spent DECIMAL (10,2) NOT NULL
);

-- Create the Location table
CREATE TABLE IF NOT EXISTS location (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL
);

-- Create the Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    transaction_id INT NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
);
