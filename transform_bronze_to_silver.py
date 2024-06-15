from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Bronze to Silver Transformation") \
    .getOrCreate()

# Leer los datos bronce desde HDFS
bronze_df = spark.read.parquet("hdfs://ruta/al/directorio/datos_bronce")

# Realizar transformaciones y agregar columnas adicionales desde datasets externos
# Ejemplo de dataset externo en formato CSV
external_df = spark.read.csv("ruta/al/dataset/externo.csv", header=True, inferSchema=True)

# Unir datasets y realizar transformaciones necesarias
silver_df = bronze_df.join(external_df, bronze_df.customer_id == external_df.id, "left") \
    .drop(external_df.id) \
    .withColumnRenamed("external_info", "additional_info")

# Escribir el DataFrame transformado en HDFS en formato Parquet (datos Silver)
silver_df.write.parquet("hdfs://ruta/al/directorio/datos_silver", mode="overwrite", compression="gzip")
