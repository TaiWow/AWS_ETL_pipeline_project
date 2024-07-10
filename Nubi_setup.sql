-- Create the database if it does not exist (run this separately)
-- CREATE DATABASE Nubi_project;

-- Connect to the Nubi_project database
-- In Adminer, select the database from a dropdown then run the below code

-- Create the Products table
CREATE TABLE IF NOT EXISTS Products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_price DECIMAL(10, 2) NOT NULL
);

-- Create the Location table
CREATE TABLE IF NOT EXISTS Location (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL
);

-- Create the Transactions table
CREATE TABLE IF NOT EXISTS Transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    transaction_time TIME NOT NULL,
    location_id INT NOT NULL,
    total_spent DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(20) NOT NULL,
    FOREIGN KEY (location_id) REFERENCES Location(location_id)
);

-- Create the Orders table
CREATE TABLE IF NOT EXISTS Orders (
    order_id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES Transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

--select t.transaction_date, count(o.product_id) as total quantity
----from Orders as o
--Left Join Transactions as t
--ON t.transaction_id = o.transaction_id


