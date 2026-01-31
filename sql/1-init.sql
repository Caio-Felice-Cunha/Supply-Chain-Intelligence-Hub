-- # Sample SQL initialization script
-- # This runs automatically when MySQL container starts
-- # Creates tables for supply chain analytics

CREATE DATABASE IF NOT EXISTS supply_chain_db;
USE supply_chain_db;

-- ============ DIMENSION TABLES ============

-- Suppliers Table
CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY AUTO_INCREMENT,
    supplier_name VARCHAR(255) NOT NULL,
    country VARCHAR(100),
    reliability_score DECIMAL(5, 2),
    lead_time_days INT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products Table
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    unit_cost DECIMAL(10, 2),
    reorder_level INT,
    supplier_id INT,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Warehouses Table
CREATE TABLE warehouses (
    warehouse_id INT PRIMARY KEY AUTO_INCREMENT,
    warehouse_name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    capacity_units INT,
    current_utilization_pct DECIMAL(5, 2)
);

-- ============ FACT TABLES ============

-- Inventory Table (Daily Snapshot)
CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT NOT NULL,
    warehouse_id INT NOT NULL,
    quantity_on_hand INT,
    quantity_reserved INT,
    snapshot_date DATE NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id),
    UNIQUE KEY unique_inventory (product_id, warehouse_id, snapshot_date)
);

-- Orders Table
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    order_date DATE NOT NULL,
    supplier_id INT NOT NULL,
    order_quantity INT NOT NULL,
    order_cost DECIMAL(15, 2),
    expected_delivery_date DATE,
    actual_delivery_date DATE,
    delivery_status VARCHAR(50),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Sales Table
CREATE TABLE sales (
    sale_id INT PRIMARY KEY AUTO_INCREMENT,
    sale_date DATE NOT NULL,
    product_id INT NOT NULL,
    warehouse_id INT NOT NULL,
    quantity_sold INT NOT NULL,
    revenue DECIMAL(15, 2),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);

-- Price History Table
CREATE TABLE price_history (
    price_history_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT NOT NULL,
    supplier_id INT NOT NULL,
    unit_price DECIMAL(10, 2),
    effective_date DATE NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Create Indexes for Performance
CREATE INDEX idx_inventory_date ON inventory(snapshot_date);
CREATE INDEX idx_sales_date ON sales(sale_date);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_product_supplier ON products(supplier_id);