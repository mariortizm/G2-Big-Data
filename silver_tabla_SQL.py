from pyspark.sql import SparkSession

# Configuración de la sesión de Spark
spark = SparkSession.builder \
    .appName("Read Parquet and Create Tables") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.17.0.2:9000") \
    .getOrCreate()

# Carga de archivos Parquet desde HDFS
silver_data_df = spark.read.parquet("hdfs://172.17.0.2:9000/silver_data")
customers_df = spark.read.parquet("hdfs://172.17.0.2:9000/customers")
employees_df = spark.read.parquet("hdfs://172.17.0.2:9000/employees")

# Guardar los DataFrames como tablas persistentes con modo "append"
silver_data_df.write.mode("append").saveAsTable("default.silver_data_table")
customers_df.write.mode("append").saveAsTable("default.customers_table")
employees_df.write.mode("append").saveAsTable("default.employees_table")

# Realizar consultas SQL directamente sobre las tablas persistentes
query = """
SELECT 
    sd.order_id AS Numero_de_orden,  
    sd.quantity_products AS Cantidad_de_productos,
    c.name AS Nombre_cliente,
    c.phone AS Telefono_cliente,
    c.address AS Direccion_cliente,
    sd.comuna AS Comuna,
    sd.codigo_postal AS Codigo_postal
FROM 
    silver_data_table sd
JOIN 
    customers_table c
ON 
    sd.customer_id = c.customer_id;
"""

result_df = spark.sql(query)

# Mostrar el resultado de la consulta
result_df.show()

# Para detener la sesión de Spark
spark.stop()


