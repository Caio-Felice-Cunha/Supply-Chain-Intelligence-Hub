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
    reliability_score DECIMAL(5, 2) CHECK (reliability_score BETWEEN 0 AND 100),
    lead_time_days INT CHECK (lead_time_days >= 0),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products Table
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    demand_class CHAR(1) DEFAULT 'C' CHECK (demand_class IN ('A', 'B', 'C')),
    unit_cost DECIMAL(10, 2) CHECK (unit_cost >= 0),
    base_price DECIMAL(10, 2) NULL CHECK (base_price IS NULL OR base_price >= 0),
    reorder_level INT CHECK (reorder_level >= 0),
    supplier_id INT NOT NULL,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Warehouses Table
CREATE TABLE warehouses (
    warehouse_id INT PRIMARY KEY AUTO_INCREMENT,
    warehouse_name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    capacity_units INT CHECK (capacity_units >= 0),
    current_utilization_pct DECIMAL(5, 2) CHECK (current_utilization_pct BETWEEN 0 AND 100)
);

-- ============ FACT TABLES ============

-- Inventory Table (Daily Snapshot)
CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT NOT NULL,
    warehouse_id INT NOT NULL,
    quantity_on_hand INT CHECK (quantity_on_hand >= 0),
    quantity_reserved INT CHECK (quantity_reserved >= 0 AND quantity_reserved <= quantity_on_hand),
    snapshot_date DATE NOT NULL,
    controlled_risk BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id),
    UNIQUE KEY unique_inventory (product_id, warehouse_id, snapshot_date)
);

-- Orders Table
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    order_date DATE NOT NULL,
    supplier_id INT NOT NULL,
    product_id INT NULL,
    warehouse_id INT NULL,
    order_quantity INT NOT NULL,
    order_cost DECIMAL(15, 2),
    expected_delivery_date DATE,
    actual_delivery_date DATE,
    delivery_status VARCHAR(50),
    anomaly_type VARCHAR(50) NOT NULL DEFAULT '',
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id),
    CHECK (order_quantity > 0),
    CHECK (order_cost >= 0)
);

-- Sales Table
CREATE TABLE sales (
    sale_id INT PRIMARY KEY AUTO_INCREMENT,
    sale_date DATE NOT NULL,
    product_id INT NOT NULL,
    warehouse_id INT NOT NULL,
    quantity_sold INT NOT NULL,
    unit_price DECIMAL(10, 2) NULL,
    revenue DECIMAL(15, 2),
    is_promotion BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_type VARCHAR(50) NOT NULL DEFAULT '',
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id),
    CHECK (quantity_sold > 0),
    CHECK (unit_price IS NULL OR unit_price >= 0),
    CHECK (revenue >= 0)
);

-- Price History Table
CREATE TABLE price_history (
    price_history_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT NOT NULL,
    supplier_id INT NOT NULL,
    unit_price DECIMAL(10, 2),
    effective_date DATE NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL DEFAULT '',
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- Create Indexes for Performance
CREATE INDEX idx_inventory_date ON inventory(snapshot_date);
CREATE INDEX idx_sales_date ON sales(sale_date);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_product_supplier ON products(supplier_id);
CREATE INDEX idx_inventory_product_warehouse ON inventory(product_id, warehouse_id);
CREATE INDEX idx_sales_product_warehouse ON sales(product_id, warehouse_id);
CREATE INDEX idx_orders_supplier_product ON orders(supplier_id, product_id);
CREATE INDEX idx_price_product_date ON price_history(product_id, effective_date);
