from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Silver to Gold Transformation") \
    .getOrCreate()

# Leer los datos Silver desde HDFS
silver_df = spark.read.parquet("hdfs://ruta/al/directorio/datos_silver")

# Realizar transformaciones adicionales para preparar datos Gold
gold_df = silver_df.groupBy("date").agg(
    {"quantity_products": "sum", "additional_info": "count"}  # Ejemplo de agregaciones
).withColumnRenamed("sum(quantity_products)", "total_products") \
 .withColumnRenamed("count(additional_info)", "info_count")

# Escribir el DataFrame Gold en formato Parquet
gold_df.write.parquet("hdfs://ruta/al/directorio/datos_gold", mode="overwrite", compression="gzip")

# Cargar los datos Gold en MariaDB para Power BI
gold_df.write \
    .format("jdbc") \
    .option("url", "jdbc:mariadb://host:port/database") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .option("dbtable", "gold_data") \
    .option("user", "username") \
    .option("password", "password") \
    .mode("overwrite") \
    .save()
