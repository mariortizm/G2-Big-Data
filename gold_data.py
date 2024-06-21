from pyspark.sql import SparkSession

# Iniciar una sesión de Spark
spark = SparkSession.builder \
    .appName("Calculating Employee Final Commission") \
    .getOrCreate()

# Leer las tablas desde HDFS en formato Parquet
df_silver = spark.read.parquet("hdfs://172.17.0.2:9000/silver_data")
df_customers = spark.read.parquet("hdfs://172.17.0.2:9000/customers")
df_employees = spark.read.parquet("hdfs://172.17.0.2:9000/employees")

# Registrar los DataFrames como tablas temporales para poder ejecutar consultas SQL sobre ellos
df_silver.createOrReplaceTempView("silver")
df_employees.createOrReplaceTempView("employees")
df_customers.createOrReplaceTempView("customers")

# Consulta SQL para calcular la comisión total ganada por empleados 
query1 = """
    SELECT s.employee_id,
           s.quantity_products * e.comission AS final_commission
    FROM silver s
    JOIN employees e
    ON s.employee_id = e.employee_id
    ORDER BY final_commission DESC
"""

# Consulta SQL para calcular los clientes que realizan mas pedidos 
query2 = """
    SELECT customer_id,
           SUM(quantity_products) AS total_quantity_products
    FROM silver
    WHERE year BETWEEN '2023' AND '2024'
    GROUP BY customer_id
    ORDER BY total_quantity_products DESC
"""

# Consulta SQL para calcular quienes son los empleados mas productivos 
query3 = """
    SELECT employee_id,
        YEAR(year) AS sale_year,
        SUM(quantity_products) AS total_quantity_products_sells
    FROM silver
    WHERE YEAR(year) BETWEEN 2023 AND 2024
    GROUP BY employee_id, YEAR(year)
    ORDER BY employee_id, sale_year
"""


# Consulta SQL para calcular la cantidad de venta por comuna 
query4 = """
    SELECT Comuna,
           SUM(quantity_products) AS total_quantity_products_sells
    FROM silver
    WHERE year BETWEEN '2023' AND '2024'
    GROUP BY Comuna
    ORDER BY total_quantity_products_sells DESC
"""

# Consulta SQL para calcular la frecuencia de clientes por comuna 
query5 = """
    SELECT Comuna,
           COUNT(DISTINCT customer_id) AS num_customers
    FROM silver
    WHERE year BETWEEN '2023' AND '2024'
    GROUP BY Comuna
    ORDER BY num_customers DESC
"""


# Lista de consultas SQL
queries = [query1, query2, query3, query4, query5]

# Ejecutar las consultas, mostrar los resultados y guardar como Parquet
for query in queries:
    result = spark.sql(query)
    result.show()
    result.write.mode("append").parquet("hdfs://172.17.0.2:9000/gold_data", compression='gzip')



# Detener la sesión de Spark
spark.stop()
