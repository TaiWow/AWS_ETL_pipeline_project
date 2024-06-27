-- Create the database if it does not exist (run this separately)
-- CREATE DATABASE Nubi_project;

-- Connect to the Nubi_project database
-- In Adminer, select the database from a dropdown then run the below code

-- Create the Products table
CREATE TABLE IF NOT EXISTS Products (
    Product_id SERIAL PRIMARY KEY,
    Product_name VARCHAR(100) NOT NULL,
    Product_price DECIMAL(10,2) NOT NULL
);

-- Create the Transactions table
CREATE TABLE IF NOT EXISTS Transactions (
    Transaction_id SERIAL PRIMARY KEY,
    Transaction_date DATE NOT NULL,
    Transaction_time TIME NOT NULL,
    Transaction_location VARCHAR(100) NOT NULL,
);

-- Create the Location table
CREATE TABLE IF NOT EXISTS Location (
    Location_id SERIAL PRIMARY KEY,
    Location VARCHAR(100) NOT NULL
);

-- Create the Orders table
CREATE TABLE IF NOT EXISTS Orders (
    Order_id SERIAL PRIMARY KEY,
    Product_id INT NOT NULL,
    Transaction_id INT NOT NULL,
    Quantity INT NOT NULL,
    FOREIGN KEY (Product_id) REFERENCES Products(Product_id),
    FOREIGN KEY (Transaction_id) REFERENCES Transactions(Transaction_id)
);
