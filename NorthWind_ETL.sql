CREATE OR REPLACE DATABASE northwind_igilchik;
CREATE OR REPLACE STAGE igilchik_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


CREATE OR REPLACE TABLE customers_staging (
    CustomerID INT, 
    CustomerName VARCHAR(255), 
    ContactName VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(100),
    PostalCode VARCHAR(50), 
    Country VARCHAR(100) 
);


CREATE OR REPLACE TABLE shippers_staging (
    shipper_id INT,
    shipper_name VARCHAR(255),
    phone VARCHAR(50)
);


CREATE OR REPLACE TABLE employees_staging (
    EmployeeID INT,
    LastName STRING,
    FirstName STRING,
    BirthDate DATE,
    Photo STRING,
    Notes STRING
);


CREATE OR REPLACE TABLE orders_staging (
    id INT,
    customerId INT,
    employeeId INT,
    orderDate STRING,
    shipperId INT
);


CREATE OR REPLACE TABLE orderdetails_staging (
    id INT,
    orderId INT,
    productId INT,
    quantity INT
);


CREATE OR REPLACE TABLE suppliers_staging (
    SupplierID INT, 
    SupplierName VARCHAR(255), 
    ContactName VARCHAR(255), 
    Address VARCHAR(255),
    City VARCHAR(100),
    PostalCode VARCHAR(50), 
    Country VARCHAR(100), 
    Phone VARCHAR(50) 
);


CREATE OR REPLACE TABLE products_staging (
    ProductID INT,
    ProductName VARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    Unit VARCHAR(255),
    Price DECIMAL(10, 2)
);


CREATE OR REPLACE TABLE categories_staging (
    CategoryID INT,
    CategoryName VARCHAR(255),
    Description VARCHAR(500)
);


COPY INTO customers_staging
FROM @NORTHWIND_IGILCHIK.PUBLIC.IGILCHIK_STAGE/customers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO shippers_staging
FROM @NORTHWIND_IGILCHIK.PUBLIC.IGILCHIK_STAGE/shippers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO employees_staging
FROM @NORTHWIND_IGILCHIK.PUBLIC.IGILCHIK_STAGE/employees.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO orders_staging
FROM @NORTHWIND_IGILCHIK.PUBLIC.IGILCHIK_STAGE/orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO orderdetails_staging
FROM @NORTHWIND_IGILCHIK.PUBLIC.IGILCHIK_STAGE/orderdetails.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO suppliers_staging
FROM @NORTHWIND_IGILCHIK.PUBLIC.IGILCHIK_STAGE/suppliers.csv
FILE_FORMAT = (TYPE = 'CSV'  FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO products_staging
FROM @NORTHWIND_IGILCHIK.PUBLIC.IGILCHIK_STAGE/products.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO categories_staging
FROM @NORTHWIND_IGILCHIK.PUBLIC.IGILCHIK_STAGE/categories.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);



SELECT * FROM customers_staging;
SELECT * FROM shippers_staging;
SELECT * FROM employees_staging;
SELECT * FROM orders_staging;
SELECT * FROM orderdetails_staging;
SELECT * FROM suppliers_staging;
SELECT * FROM products_staging;
SELECT * FROM categories_staging;



CREATE OR REPLACE TABLE dim_customers (
    customer_id INT,
    customer_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    postal_code VARCHAR(50),
    country VARCHAR(100)
);

INSERT INTO dim_customers (customer_id, customer_name, address, city, postal_code, country)
SELECT 
    CustomerID AS customer_id, 
    CustomerName AS customer_name,
    Address AS address,
    City AS city,
    PostalCode AS postal_code,
    Country AS country
FROM customers_staging;


SELECT * FROM dim_customers;


CREATE OR REPLACE TABLE dim_suppliers (
    supplier_id INT,
    supplier_name VARCHAR(255), 
    address VARCHAR(255), 
    city VARCHAR(100),
    postal_code VARCHAR(50), 
    country VARCHAR(100), 
    phone VARCHAR(50) 
);

INSERT INTO dim_suppliers (supplier_id, supplier_name, address, city, postal_code, country, phone)
SELECT 
    SupplierID AS supplier_id,
    SupplierName AS supplier_name,
    Address AS address,
    City AS city,
    PostalCode AS postal_code,
    Country AS country,
    Phone AS phone
FROM suppliers_staging;


SELECT * FROM dim_suppliers;


CREATE OR REPLACE TABLE dim_employees (
    employee_id INT, 
    full_name VARCHAR(255), 
    notes TEXT 
);

INSERT INTO dim_employees (employee_id, full_name, notes)
SELECT 
    EmployeeID AS employee_id,
    CONCAT(FirstName, ' ', LastName) AS full_name,
    Notes AS notes
FROM employees_staging;


SELECT * FROM dim_employees;


CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY TO_DATE(TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS'))) AS date_id,
    TO_DATE(TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS full_date, 
    CASE DATE_PART('dow', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) 
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS weekday 
FROM orders_staging;


SELECT * FROM dim_date;


CREATE OR REPLACE TABLE dim_products (
    product_id INT,
    productname VARCHAR(255),
    category VARCHAR(255),
    description VARCHAR(255)
);

INSERT INTO dim_products (product_id, productname, category, description)
SELECT 
    p.ProductID,
    p.ProductName,
    c.CategoryName, 
    c.Description AS CategoryDescription
FROM products_staging p
JOIN categories_staging c
ON p.CategoryID = c.CategoryID;


SELECT * FROM dim_products;


CREATE OR REPLACE TABLE dim_shippers (
    shipper_id INT,
    shipper_name VARCHAR(255),
    phone VARCHAR(50)
);

INSERT INTO dim_shippers (shipper_id, shipper_name, phone)
SELECT 
    shipper_id,
    shipper_name,
    phone
FROM shippers_staging;


SELECT * FROM dim_shippers;


CREATE OR REPLACE TABLE bridge_orders_products AS
SELECT 
    id,
    orderid,
    productid,
FROM orderdetails_staging;


SELECT * FROM bridge_orders_products;


CREATE OR REPLACE TABLE fact_orders AS
SELECT
    o.id AS order_id,
    od.quantity AS quantity,
    CAST(p.price AS DECIMAL(10,2)) AS price, 
    (od.quantity * CAST(p.price AS DECIMAL(10,2))) AS total_price, 
    o.customerid AS customer_id,
    p.supplierid AS supplier_id,
    o.employeeid AS employee_id,
    s.shipper_id AS shipper_id,
    d.date_id AS date_id
FROM orders_staging o
JOIN bridge_orders_products b ON o.id = b.orderid
JOIN dim_employees e ON o.employeeid = e.employee_id
JOIN dim_customers c ON o.customerid = c.customer_id
JOIN orderdetails_staging od ON o.id = od.orderid
JOIN products_staging p ON od.productid = p.ProductID
JOIN dim_shippers s ON o.shipperid = s.shipper_id
JOIN dim_date d ON CAST(o.orderdate AS DATE) = d.full_date;


SELECT * FROM fact_orders;


DROP TABLE IF EXISTS PUBLIC.customers_staging;
DROP TABLE IF EXISTS PUBLIC.shippers_staging;
DROP TABLE IF EXISTS PUBLIC.employees_staging;
DROP TABLE IF EXISTS PUBLIC.orders_staging;
DROP TABLE IF EXISTS PUBLIC.orderdetails_staging;
DROP TABLE IF EXISTS PUBLIC.suppliers_staging;
DROP TABLE IF EXISTS PUBLIC.products_staging;
DROP TABLE IF EXISTS PUBLIC.categories_staging;


SELECT 
    d.full_date,
    COUNT(f.order_id) AS total_orders
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date
ORDER BY d.full_date;


SELECT 
    s.supplier_name AS supplier_name,
    COUNT(f.order_id) AS total_orders
FROM PUBLIC.fact_orders f
JOIN PUBLIC.dim_suppliers s ON f.supplier_id = s.supplier_id
GROUP BY s.supplier_name
ORDER BY total_orders DESC;


SELECT 
    e.full_name AS employee_name, 
    ROUND(AVG(f.total_price), 1) AS avg_order_price
FROM PUBLIC.fact_orders f
JOIN PUBLIC.dim_employees e ON f.employee_id = e.employee_id
GROUP BY e.full_name
ORDER BY avg_order_price DESC;


SELECT 
    c.city AS city,
    COUNT(f.order_id) AS total_orders
FROM PUBLIC.fact_orders f
JOIN PUBLIC.dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.city
ORDER BY total_orders DESC;


SELECT 
    CASE 
        WHEN EXTRACT(MONTH FROM d.full_date) = 1 THEN 'January'
        WHEN EXTRACT(MONTH FROM d.full_date) = 2 THEN 'February'
        WHEN EXTRACT(MONTH FROM d.full_date) = 3 THEN 'March'
        WHEN EXTRACT(MONTH FROM d.full_date) = 4 THEN 'April'
        WHEN EXTRACT(MONTH FROM d.full_date) = 5 THEN 'May'
        WHEN EXTRACT(MONTH FROM d.full_date) = 6 THEN 'June'
        WHEN EXTRACT(MONTH FROM d.full_date) = 7 THEN 'July'
        WHEN EXTRACT(MONTH FROM d.full_date) = 8 THEN 'August'
        WHEN EXTRACT(MONTH FROM d.full_date) = 9 THEN 'September'
        WHEN EXTRACT(MONTH FROM d.full_date) = 10 THEN 'October'
        WHEN EXTRACT(MONTH FROM d.full_date) = 11 THEN 'November'
        WHEN EXTRACT(MONTH FROM d.full_date) = 12 THEN 'December'
    END AS order_month,
    COUNT(f.order_id) AS total_orders
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY EXTRACT(MONTH FROM d.full_date)
ORDER BY EXTRACT(MONTH FROM d.full_date);



SELECT 
    COUNT(f.order_id) AS total_orders,
    SUM(f.total_price) AS total_revenue
FROM PUBLIC.fact_orders f
GROUP BY f.employee_id;