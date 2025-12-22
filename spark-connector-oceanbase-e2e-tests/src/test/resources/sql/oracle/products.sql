-- Copyright 2024 OceanBase.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Oracle mode test data initialization script
-- This script creates test tables and data for OceanBase Oracle mode testing

-- Create test schema if not exists
CREATE USER test_schema IDENTIFIED BY 'password';
GRANT CONNECT, RESOURCE TO test_schema;

-- Switch to test schema
ALTER SESSION SET CURRENT_SCHEMA = test_schema;

-- Create products table with various Oracle data types
CREATE TABLE products (
    product_id NUMBER(10) NOT NULL,
    product_name VARCHAR2(255) NOT NULL,
    description CLOB,
    price NUMBER(19,4),
    category VARCHAR2(100),
    is_active NUMBER(1) DEFAULT 1,
    created_date DATE DEFAULT SYSDATE,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    product_code RAW(16),
    CONSTRAINT pk_products PRIMARY KEY (product_id),
    CONSTRAINT uk_products_code UNIQUE (product_code)
);

-- Create partitioned products table
CREATE TABLE products_partitioned (
    product_id NUMBER(10) NOT NULL,
    product_name VARCHAR2(255) NOT NULL,
    price NUMBER(19,4),
    category VARCHAR2(100),
    created_date DATE DEFAULT SYSDATE,
    CONSTRAINT pk_products_partitioned PRIMARY KEY (product_id)
) PARTITION BY HASH(product_id) PARTITIONS 4;

-- Create customers table
CREATE TABLE customers (
    customer_id NUMBER(10) NOT NULL,
    customer_name VARCHAR2(255) NOT NULL,
    email VARCHAR2(255),
    phone VARCHAR2(20),
    address CLOB,
    registration_date DATE DEFAULT SYSDATE,
    CONSTRAINT pk_customers PRIMARY KEY (customer_id),
    CONSTRAINT uk_customers_email UNIQUE (email)
);

-- Create orders table with foreign key
CREATE TABLE orders (
    order_id NUMBER(10) NOT NULL,
    customer_id NUMBER(10) NOT NULL,
    order_date DATE DEFAULT SYSDATE,
    total_amount NUMBER(19,4),
    status VARCHAR2(50) DEFAULT 'PENDING',
    notes CLOB,
    CONSTRAINT pk_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Create order_items table
CREATE TABLE order_items (
    order_item_id NUMBER(10) NOT NULL,
    order_id NUMBER(10) NOT NULL,
    product_id NUMBER(10) NOT NULL,
    quantity NUMBER(10) NOT NULL,
    unit_price NUMBER(19,4) NOT NULL,
    total_price NUMBER(19,4) NOT NULL,
    CONSTRAINT pk_order_items PRIMARY KEY (order_item_id),
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Insert test data
INSERT INTO products (product_id, product_name, description, price, category, product_code) VALUES
(1, 'Laptop Computer', 'High-performance laptop with 16GB RAM and 512GB SSD', 1299.99, 'Electronics', UTL_RAW.CAST_TO_RAW('LAPTOP001')),
(2, 'Wireless Mouse', 'Ergonomic wireless mouse with USB receiver', 29.99, 'Electronics', UTL_RAW.CAST_TO_RAW('MOUSE001')),
(3, 'Office Chair', 'Comfortable ergonomic office chair with lumbar support', 199.99, 'Furniture', UTL_RAW.CAST_TO_RAW('CHAIR001')),
(4, 'Coffee Mug', 'Ceramic coffee mug with company logo', 12.99, 'Accessories', UTL_RAW.CAST_TO_RAW('MUG001')),
(5, 'Notebook', 'Spiral-bound notebook with 200 pages', 8.99, 'Stationery', UTL_RAW.CAST_TO_RAW('NOTE001'));

INSERT INTO products_partitioned (product_id, product_name, price, category) VALUES
(1, 'Laptop Computer', 1299.99, 'Electronics'),
(2, 'Wireless Mouse', 29.99, 'Electronics'),
(3, 'Office Chair', 199.99, 'Furniture'),
(4, 'Coffee Mug', 12.99, 'Accessories'),
(5, 'Notebook', 8.99, 'Stationery');

INSERT INTO customers (customer_id, customer_name, email, phone, address) VALUES
(1, 'John Smith', 'john.smith@email.com', '555-0101', '123 Main Street, Anytown, USA'),
(2, 'Jane Doe', 'jane.doe@email.com', '555-0102', '456 Oak Avenue, Somewhere, USA'),
(3, 'Bob Johnson', 'bob.johnson@email.com', '555-0103', '789 Pine Road, Elsewhere, USA'),
(4, 'Alice Brown', 'alice.brown@email.com', '555-0104', '321 Elm Street, Nowhere, USA'),
(5, 'Charlie Wilson', 'charlie.wilson@email.com', '555-0105', '654 Maple Drive, Anywhere, USA');

INSERT INTO orders (order_id, customer_id, total_amount, status, notes) VALUES
(1, 1, 1329.98, 'COMPLETED', 'Order placed online'),
(2, 2, 229.98, 'PENDING', 'Order awaiting payment'),
(3, 3, 212.98, 'SHIPPED', 'Order shipped via express delivery'),
(4, 4, 21.98, 'COMPLETED', 'Order completed successfully'),
(5, 5, 8.99, 'CANCELLED', 'Order cancelled by customer');

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, total_price) VALUES
(1, 1, 1, 1, 1299.99, 1299.99),
(2, 1, 2, 1, 29.99, 29.99),
(3, 2, 1, 1, 1299.99, 1299.99),
(4, 2, 3, 1, 199.99, 199.99),
(5, 3, 3, 1, 199.99, 199.99),
(6, 3, 2, 1, 29.99, 29.99),
(7, 4, 2, 1, 29.99, 29.99),
(8, 4, 4, 1, 12.99, 12.99),
(9, 5, 5, 1, 8.99, 8.99);

-- Create indexes for better performance
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);

-- Create a view for order summary
CREATE VIEW order_summary AS
SELECT 
    o.order_id,
    c.customer_name,
    o.order_date,
    o.total_amount,
    o.status,
    COUNT(oi.order_item_id) as item_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, c.customer_name, o.order_date, o.total_amount, o.status;

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON products TO test_schema;
GRANT SELECT, INSERT, UPDATE, DELETE ON products_partitioned TO test_schema;
GRANT SELECT, INSERT, UPDATE, DELETE ON customers TO test_schema;
GRANT SELECT, INSERT, UPDATE, DELETE ON orders TO test_schema;
GRANT SELECT, INSERT, UPDATE, DELETE ON order_items TO test_schema;
GRANT SELECT ON order_summary TO test_schema;
