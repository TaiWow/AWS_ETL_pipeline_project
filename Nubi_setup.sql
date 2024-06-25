CREATE DATABASE IF NOT EXISTS Nubi_project;

CREATE TABLE IF NOT EXISTS Products (
Product_id INT NOT NULL AUTO_INCREMENT,
Product_name VARCHAR(100) NOT NULL,
Product_price DECIMAL(10,2) NOT NULL,
PRIMARY KEY (Product_id),
);

CREATE TABLE IF NOT EXISTS Transactions (
Transaction_id INT NOT NULL AUTO_INCREMENT,
Transaction_date DATE NOT NULL,
Transaction_time TIME NOT NULL,
Total_spent DECIMAL(10,2) NOT NULL,
PRIMARY KEY (Transaction_id),
);

CREATE TABLE IF NOT EXISTS Customers (
Customer_id INT NOT NULL AUTO_INCREMENT,
Customer_location VARCHAR(100) NOT NULL,
PRIMARY KEY (Customer_id),
);

CREATE TABLE IF NOT EXISTS Orders (
Order_id INT NOT NULL AUTO_INCREMENT,
Product_id INT NOT  NULL,
Transaction_id INT NOT NULL,
PRIMARY KEY (Order_id),
FOREIGN KEY (Product_id) REFERENCES Products(Product_id),
FOREIGN KEY (Transaction_id) REFERENCES Transactions(Transaction_id),
);


