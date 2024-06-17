from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Leer Parquet de HDFS") \
    .getOrCreate()

# Ruta del directorio en HDFS donde están los archivos Parquet
hdfs_directory = "hdfs://172.17.0.2:9000/merged_data"

# Leer todos los archivos Parquet del directorio en un DataFrame
df = spark.read.parquet(hdfs_directory)

# Mostrar el esquema del DataFrame
df.printSchema()

# Mostrar algunas filas del DataFrame
df.show()