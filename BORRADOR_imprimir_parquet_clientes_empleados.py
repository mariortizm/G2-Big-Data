from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Leer Parquet de HDFS") \
    .getOrCreate()

# Ruta del directorio en HDFS donde están los archivos Parquet
customers_hdfs_directory = "hdfs://172.17.0.2:9000/customers"
employees_hdfs_directory = "hdfs://172.17.0.2:9000/employees"

# Leer todos los archivos Parquet del directorio en un DataFrame
customers_df = spark.read.parquet(customers_hdfs_directory)
employees_df = spark.read.parquet(employees_hdfs_directory)

# Mostrar el esquema de los DataFrames
print("Esquema del DataFrame de customers:")
customers_df.printSchema()

print("Esquema del DataFrame de employees:")
employees_df.printSchema()

# Mostrar algunas filas de los DataFrames
print("Algunas filas del DataFrame de customers:")
customers_df.show()

print("Algunas filas del DataFrame de employees:")
employees_df.show()
