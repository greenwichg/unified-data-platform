-- Zomato Database Schema
-- Source tables for Pipeline 1 (Spark JDBC) and Pipeline 2 (Debezium CDC)

USE zomato;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100) NOT NULL,
    signup_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_pro_member BOOLEAN DEFAULT FALSE,
    preferred_cuisine JSON,
    total_orders BIGINT DEFAULT 0,
    average_order_value DECIMAL(10, 2) DEFAULT 0.00,
    last_order_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_city (city),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB;

-- Restaurants table
CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    area VARCHAR(255),
    cuisine_types JSON,
    rating FLOAT DEFAULT 0,
    total_reviews INT DEFAULT 0,
    avg_cost_for_two DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    latitude DOUBLE,
    longitude DOUBLE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_city (city),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB;

-- Menu Items table
CREATE TABLE IF NOT EXISTS menu_items (
    item_id VARCHAR(36) PRIMARY KEY,
    restaurant_id VARCHAR(36) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    cuisine_type VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    is_vegetarian BOOLEAN DEFAULT FALSE,
    is_available BOOLEAN DEFAULT TRUE,
    preparation_time_mins INT DEFAULT 30,
    rating FLOAT,
    total_orders BIGINT DEFAULT 0,
    image_url VARCHAR(500),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(restaurant_id),
    INDEX idx_restaurant (restaurant_id),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB;

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    restaurant_id VARCHAR(36) NOT NULL,
    status ENUM('PLACED', 'CONFIRMED', 'PREPARING', 'READY', 'PICKED_UP', 'DELIVERING', 'DELIVERED', 'CANCELLED') NOT NULL,
    subtotal DECIMAL(10, 2) NOT NULL,
    tax DECIMAL(10, 2) NOT NULL,
    delivery_fee DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    delivery_latitude DOUBLE,
    delivery_longitude DOUBLE,
    delivery_address TEXT,
    city VARCHAR(100) NOT NULL,
    pincode VARCHAR(10),
    estimated_delivery_mins INT,
    actual_delivery_mins INT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(restaurant_id),
    INDEX idx_user (user_id),
    INDEX idx_restaurant (restaurant_id),
    INDEX idx_status (status),
    INDEX idx_city (city),
    INDEX idx_created_at (created_at),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB;

-- Payments table
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    payment_status ENUM('INITIATED', 'PROCESSING', 'COMPLETED', 'FAILED', 'REFUNDED') NOT NULL,
    transaction_id VARCHAR(100),
    gateway_response JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    INDEX idx_order (order_id),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB;

-- Promotions table
CREATE TABLE IF NOT EXISTS promotions (
    promo_id VARCHAR(36) PRIMARY KEY,
    promo_code VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    discount_type ENUM('PERCENTAGE', 'FLAT', 'CASHBACK', 'FREE_DELIVERY') NOT NULL,
    discount_value DECIMAL(10, 2) NOT NULL,
    min_order_value DECIMAL(10, 2) DEFAULT 0,
    max_discount DECIMAL(10, 2),
    valid_from DATETIME NOT NULL,
    valid_until DATETIME NOT NULL,
    applicable_restaurants JSON,
    applicable_cities JSON,
    usage_count BIGINT DEFAULT 0,
    max_usage BIGINT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_promo_code (promo_code),
    INDEX idx_active (is_active),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB;

-- Debezium signal table (for incremental snapshots)
CREATE TABLE IF NOT EXISTS debezium_signal (
    id VARCHAR(42) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048) NULL
) ENGINE=InnoDB;

-- Grant Debezium user permissions
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium_pass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
