-- Create sample schema and tables
CREATE SCHEMA sales;
GO

-- Create customers table
CREATE TABLE sales.customers (
    customer_id INT PRIMARY KEY,
    customer_name NVARCHAR(100) NOT NULL,
    email NVARCHAR(100),
    city NVARCHAR(50),
    state NVARCHAR(50),
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Create orders table
CREATE TABLE sales.orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME2 NOT NULL,
    total_amount DECIMAL(18, 2) NOT NULL,
    order_status NVARCHAR(20) NOT NULL,
    updated_date DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (customer_id) REFERENCES sales.customers(customer_id)
);

-- Insert sample data
INSERT INTO sales.customers (customer_id, customer_name, email, city, state)
VALUES 
    (1, 'John Smith', 'john.smith@email.com', 'Seattle', 'WA'),
    (2, 'Jane Doe', 'jane.doe@email.com', 'Portland', 'OR'),
    (3, 'Bob Johnson', 'bob.johnson@email.com', 'San Francisco', 'CA');

INSERT INTO sales.orders (order_id, customer_id, order_date, total_amount, order_status)
VALUES 
    (1001, 1, '2024-01-15 10:30:00', 1250.00, 'Completed'),
    (1002, 2, '2024-01-16 14:20:00', 750.50, 'Completed'),
    (1003, 1, '2024-01-17 09:15:00', 2100.00, 'Completed');

-- Create index for better performance
CREATE INDEX idx_orders_updated_date ON sales.orders(updated_date);
GO
