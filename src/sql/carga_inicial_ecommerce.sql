CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    categoria VARCHAR(255) NOT NULL,
    reference_id VARCHAR(50) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL
);


CREATE TABLE IF NOT EXISTS customers (
    id VARCHAR(50) PRIMARY KEY,
    created_at DATE NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    tax_id VARCHAR(20) NOT NULL,
    address TEXT NOT NULL,
    phones TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(50) PRIMARY KEY,
    reference_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    shipping TEXT NOT NULL,
    items TEXT NOT NULL,
    customer TEXT NOT NULL,
    charges TEXT NOT NULL
);


-- Insert into customers if the table is empty
DO $$ 
BEGIN
    IF (SELECT COUNT(*) FROM customers) = 0 THEN
        -- Load data from customers.csv file into the customers table
         COPY customers(id, created_at, name, email, tax_id, address, phones) 
        FROM '/src/customers.csv' DELIMITER ',' CSV HEADER;
    END IF;
END $$;

-- Insert into products if the table is empty
DO $$ 
BEGIN
    IF (SELECT COUNT(*) FROM products) = 0 THEN
        -- Load data from products.csv file into the orders table
        COPY products(name, categoria, reference_id, unit_price) 
        FROM '/src/products.csv' DELIMITER ',' CSV HEADER;
    END IF;
END $$;
