from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Streaming to Parquet with Compression") \
    .getOrCreate()

# Configurar compresión Parquet a Gzip
spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

# Definir el esquema del JSON
schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("quantity_products", IntegerType(), True),
    StructField("order_id", StringType(), True)
])

# Leer desde el socket en streaming
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Convertir las líneas de JSON a DataFrame estructurado
json_df = lines.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Escribir el DataFrame en HDFS en formato Parquet con compresión
query = json_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://ruta/al/directorio/datos_bronce") \
    .option("checkpointLocation", "hdfs://ruta/al/directorio/checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
