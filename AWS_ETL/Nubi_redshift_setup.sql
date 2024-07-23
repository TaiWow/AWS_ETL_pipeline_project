DROP TABLE IF EXISTS Orders CASCADE;
DROP TABLE IF EXISTS Transactions CASCADE;
DROP TABLE IF EXISTS Products CASCADE;
DROP TABLE IF EXISTS Location CASCADE;

-- Create the Products table
CREATE TABLE IF NOT EXISTS Products (
    product_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_price DECIMAL(10, 2) NOT NULL,
    UNIQUE (product_name, product_price)
);
 
-- Create the Location table
CREATE TABLE IF NOT EXISTS Location (
    location_id BIGINT IDENTITY(1,1)PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL
    UNIQUE (location_name)
);
 
-- Create the Transactions table
CREATE TABLE IF NOT EXISTS Transactions (
    transaction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    transaction_time TIME NOT NULL,
    location_id BIGINT NOT NULL,
    total_spent DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(20) NOT NULL, 
    FOREIGN KEY (location_id) REFERENCES Location(location_id)
);
 
-- Create the Orders table
CREATE TABLE IF NOT EXISTS Orders (
    order_id BIGINT  IDENTITY(1,1) PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES Transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

