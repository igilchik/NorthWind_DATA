CREATE OR REPLACE DATABASE northwind_igilchik;
CREATE OR REPLACE STAGE igilchik_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


CREATE OR REPLACE TABLE customers_staging (
    id INT,
    customername STRING,
    contactname STRING,
    address STRING,
    city STRING,
    postalCode STRING,
    country STRING
);


CREATE OR REPLACE TABLE shippers_staging (
    id INT,
    shippername STRING,
    phone STRING
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
    id INT,
    supplierName STRING,
    contactName STRING,
    address STRING,
    city STRING,
    postalCode STRING,
    country STRING,
    phone STRING  
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



CREATE OR REPLACE TABLE dim_locations (
    location_id INT,
    address VARCHAR(100),
    city VARCHAR(50),
    postalcode VARCHAR(20),
    country VARCHAR(50)
);

INSERT INTO dim_locations (location_id, address, city, postalcode, country)
SELECT ROW_NUMBER() OVER (ORDER BY address) AS location_id,
       address, city, postalcode, country
FROM (
    SELECT DISTINCT address, city, postalcode, country FROM suppliers_staging
    UNION
    SELECT DISTINCT address, city, postalcode, country FROM customers_staging
) locations;


SELECT * FROM dim_locations;


CREATE OR REPLACE TABLE dim_customers (
    customer_id INT,
    name VARCHAR(100),
    location_id INT
);

INSERT INTO dim_customers (customer_id, name, location_id)
SELECT 
    c.id AS customer_id,
    c.customername AS name,
    l.location_id
FROM customers_staging c
JOIN dim_locations l
ON c.address = l.address 
   AND c.city = l.city
   AND c.postalCode = l.postalcode
   AND c.country = l.country;


SELECT * FROM dim_customers;


CREATE OR REPLACE TABLE dim_suppliers (
    supplier_id INT,
    name VARCHAR(100),
    location_id INT
);

INSERT INTO dim_suppliers (supplier_id, name, location_id)
SELECT s.id AS supplier_id,
       s.suppliername AS name,
       l.location_id
FROM suppliers_staging s
JOIN dim_locations l
  ON s.address = l.address AND s.city = l.city
  AND s.postalcode = l.postalcode AND s.country = l.country;


SELECT * FROM dim_suppliers;


CREATE OR REPLACE TABLE dim_employees (
    employee_id INT,
    full_name VARCHAR(100)
);

INSERT INTO dim_employees (employee_id, full_name)
SELECT 
    e.EMPLOYEEID AS employee_id, 
    CONCAT(e.FIRSTNAME, ' ', e.LASTNAME) AS full_name
FROM employees_staging e;


SELECT * FROM dim_employees;


CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY TO_DATE(TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS'))) AS date_id,
    DATE_PART('year', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS year,
    DATE_PART('month', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS month,
    DATE_PART('day', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS day,
    TO_DATE(TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) AS full_date,
    CASE DATE_PART('dow', TO_TIMESTAMP(orderdate, 'YYYY-MM-DD HH24:MI:SS')) 
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS dayOfWeekAsString
FROM orders_staging;


SELECT * FROM dim_date;


CREATE OR REPLACE TABLE dim_products (
    product_id INT,
    productname VARCHAR(255),
    category VARCHAR(255),
    description VARCHAR(255),
    unit VARCHAR(45)
);


INSERT INTO dim_products (product_id, productname, category, description, unit)
SELECT 
    p.ProductID,
    p.ProductName,
    c.CategoryName, 
    c.Description AS CategoryDescription, 
    p.Unit
FROM products_staging p
JOIN categories_staging c
ON p.CategoryID = c.CategoryID; 


SELECT * FROM dim_products;


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
    d.date_id AS date_id
FROM orders_staging o
JOIN bridge_orders_products b ON o.id = b.orderid
JOIN dim_employees e ON o.employeeid = e.employee_id
JOIN dim_customers c ON o.customerid = c.customer_id
JOIN orderdetails_staging od ON o.id = od.orderid
JOIN products_staging p ON od.productid = p.ProductID
JOIN dim_date d ON DATE_PART('year', TO_TIMESTAMP(o.orderdate, 'YYYY-MM-DD HH24:MI:SS')) = d.year
                  AND DATE_PART('month', TO_TIMESTAMP(o.orderdate, 'YYYY-MM-DD HH24:MI:SS')) = d.month
                  AND DATE_PART('day', TO_TIMESTAMP(o.orderdate, 'YYYY-MM-DD HH24:MI:SS')) = d.day;


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
    s.name AS supplier_name, 
    COUNT(f.order_id) AS total_orders
FROM PUBLIC.fact_orders f
JOIN PUBLIC.dim_suppliers s ON f.supplier_id = s.supplier_id
GROUP BY s.name
ORDER BY total_orders DESC;


SELECT 
    e.full_name AS employee_name, 
    ROUND(AVG(f.total_price), 1) AS avg_order_price
FROM PUBLIC.fact_orders f
JOIN PUBLIC.dim_employees e ON f.employee_id = e.employee_id
GROUP BY e.full_name
ORDER BY avg_order_price DESC;


SELECT 
    l.city,
    COUNT(f.order_id) AS total_orders
FROM PUBLIC.fact_orders f
JOIN PUBLIC.dim_customers c ON f.customer_id = c.customer_id
JOIN PUBLIC.dim_locations l ON c.location_id = l.location_id
GROUP BY l.city
ORDER BY total_orders DESC;


SELECT 
    CASE d.month
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS order_month,
    COUNT(f.order_id) AS total_orders
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.month
ORDER BY d.month;


SELECT 
    COUNT(f.order_id) AS total_orders,
    SUM(f.total_price) AS total_revenue
FROM PUBLIC.fact_orders f
GROUP BY f.employee_id;