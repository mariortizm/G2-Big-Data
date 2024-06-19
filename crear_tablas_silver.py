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

# Creación de tablas temporales para SQL
silver_data_df.createOrReplaceTempView("silver_data")
customers_df.createOrReplaceTempView("customers")
employees_df.createOrReplaceTempView("employees")

# Consulta SQL
query = """
SELECT 
    sd.order_id AS Numero_de_orden, 
    sd.customer_id AS Identificacion_cliente, 
    sd.codigo_postal AS Codigo_postal, 
    sd.quantity_products AS Cantidad_de_productos,
    c.direccion AS Direccion_cliente
FROM 
    silver_data sd
JOIN 
    customers c
ON 
    sd.customer_id = c.customer_id;
"""

result_df = spark.sql(query)

# Mostrar el resultado de la consulta
result_df.show()

# Para detener la sesión de Spark
spark.stop()

