CREATE EXTERNAL TABLE sales_events (
    latitude DOUBLE,
    longitude DOUBLE,
    `date` STRING,
    customer_id INT,
    employee_id INT,
    quantity_products INT,
    order_id STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/workspace/bronze/';

