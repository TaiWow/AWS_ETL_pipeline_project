CREATE DATABASE IF NOT EXIST Nubi_project;

CREATE TABLE Products (
Product_id INT NOT NULL AUTO_INCREMENT,
Product_name VARCHAR(100) NOT NULL,
Product_size VARCHAR(100) NOT NULL,
Product_price DECIMAL(10,2) NOT NULL,
PRIMARY KEY (Product_id)
);

CREATE TABLE Transactions (
Transaction_id INT NOT NULL AUTO_INCREMENT,
Transaction_date DATE NOT NULL,
Transaction_time TIME NOT NULL,
Total_spent DECIMAL(10,2) NOT NULL,
PRIMARY KEY (Transaction_id)
);

CREATE TABLE Customers (
Customer_id INT NOT NULL AUTO_INCREMENT,
Customer_name VARCHAR(100) NOT NULL,
Customer_location VARCHAR(100) NOT NULL,
PRIMARY KEY (Transaction_id)
);

CREATE TABLE Orders (
Order_id INT NOT NULL AUTO_INCREMENT
Product_id INT NOT  NULL,
Transaction_id INT NOT NULL,
PRIMARY KEY (Order_id)
FOREIGN KEY (Product_id) REFERENCES Orders(Order_id)
FOREIGN KEY (Transaction_id) REFERENCES Transactions(Transaction_id)
);


