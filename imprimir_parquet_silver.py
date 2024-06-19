from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Leer Parquet de HDFS") \
    .getOrCreate()

# Ruta del directorio en HDFS donde están los archivos Parquet
hdfs_directory = "hdfs://172.17.0.2:9000/silver_data"
customers_path = "hdfs://172.17.0.2:9000/customers"
employees_path = "hdfs://172.17.0.2:9000/employees"

# Leer todos los archivos Parquet del directorio en un DataFrame
df = spark.read.parquet(hdfs_directory)

# Mostrar el esquema del DataFrame
df.printSchema()

# Mostrar algunas filas del DataFrame
df.show()

# Leer y mostrar el archivo customers.parquet
customers_df = spark.read.parquet(customers_path)
print("Customers DataFrame Schema:")
customers_df.printSchema()
print("Customers DataFrame Content:")
customers_df.show()

# Leer y mostrar el archivo employees.parquet
employees_df = spark.read.parquet(employees_path)
print("Employees DataFrame Schema:")
employees_df.printSchema()
print("Employees DataFrame Content:")
employees_df.show()

spark.stop()
