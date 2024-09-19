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

CREATE TABLE IF NOT EXISTS ft_orders (
	id VARCHAR(50) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	shipping VARCHAR(50) NOT NULL,
	customer VARCHAR(50) NOT NULL,
	item_references VARCHAR(50) NOT NULL,
	charges_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	charges_paid_at TIMESTAMP WITH TIME ZONE NOT NULL,
	charge_value numeric(10, 2) NOT NULL,
	payment_method_type VARCHAR(50) NOT NULL
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

-- Insert into ft_orders if the table is empty
DO $$ 
BEGIN
    IF (SELECT COUNT(*) FROM ft_orders) = 0 THEN
        -- Load data from products.csv file into the orders table
        COPY ft_orders(id, created_at, shipping, customer, item_references, charges_created_at, charges_paid_at, charge_value, payment_method_type) 
        FROM '/src/ft_orders.csv' DELIMITER ',' CSV HEADER;
    END IF;
END $$;