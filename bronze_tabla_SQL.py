from pyspark.sql import SparkSession

# Configuración de la sesión de Spark
spark = SparkSession.builder \
    .appName("Read Parquet and Create Tables") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.17.0.2:9000") \
    .getOrCreate()

# Carga de archivos Parquet desde HDFS
bronze_data_df = spark.read.parquet("hdfs://172.17.0.2:9000/bronze_data")

# Guardar el DataFrame como tabla persistente con modo "append"
bronze_data_df.write.mode("append").saveAsTable("default.bronze_data_table")

# Realizar consultas SQL directamente sobre la tabla persistente
query = """
SELECT * FROM bronze_data_table;
"""

result_df = spark.sql(query)

# Mostrar el resultado de la consulta
result_df.show()

# Para detener la sesión de Spark
spark.stop()



