import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StringType, StructType, StructField, FloatType, IntegerType
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from pyproj import Transformer

# Función para ejecutar comandos de shell
def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise Exception(f"Command failed with error {stderr.decode('utf-8')}")
    return stdout.decode('utf-8')

# Crear directorio en HDFS y subir archivos locales
run_command("hdfs dfs -mkdir -p /user/root/base.data")
run_command("hdfs dfs -put ./base.data/customers.parquet /user/root/base.data/")
run_command("hdfs dfs -put ./base.data/employees.parquet /user/root/base.data/")

# Inicializar Spark
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .getOrCreate()

# Leer el archivo parquet desde HDFS
# df = spark.read.parquet("hdfs://172.17.0.2:9000/merged_data")
#--------------------------------------------------------------------------------------------
# Definir el esquema inicial del DataFrame
schema_initial = StructType([
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("quantity_products", IntegerType(), True),
    StructField("order_id", StringType(), True)
])

# Simulación de datos de ejemplo
data = [
    (6.222290, -75.591675, "2024-04-15 20:48:39", 5595, 6740, 66, "9e3320d3-4b26-4a6b-8a1e-9e5a9f6b8c2f"),
    (6.218872, -75.589478, "2024-03-15 20:48:19", 7957, 6467, 81, "0ff33fcb-c2f0-4de3-bbf8-d6b9b0b56c2f"),
    (6.2578964, -75.58538, "2021-06-15 20:48:41", 2042, 1655, 41, "ad74866e-0822-4ab8-9e3c-5f4b8c9a2d2f"),
    (6.182959, -75.55851, "2020-06-15 20:48:21", 5412, 2558, 81, "89692ac8-e606-4cf0-bf92-3f9a4b5c9d2f"),
    (6.259521, -75.561829, "2022-06-15 20:48:43", 6459, 4646, 29, "dec974fc-c2c7-47e3-bb1c-4e9b0b6c9e2f")
]

# Crear un DataFrame de ejemplo
df = spark.createDataFrame(data, schema=schema_initial)
#--------------------------------------------------------------------------------------------

# Transformación 1: Separar la columna 'date' en día, mes, año y tiempo
df = df.withColumn("day", split(col("date"), " ")[0].substr(9, 2).cast(StringType())) \
       .withColumn("month", split(col("date"), " ")[0].substr(6, 2).cast(StringType())) \
       .withColumn("year", split(col("date"), " ")[0].substr(0, 4).cast(StringType())) \
       .withColumn("time", split(col("date"), " ")[1].cast(StringType()))

# Leer el archivo de comunas
neighborhoods_gdf = gpd.read_parquet("./base.data/medellin_neighborhoods.parquet")

# Leer el archivo GeoJSON de códigos postales
codigo_postal_gdf = gpd.read_file("./base.data/codigo_postal.geojson")

# Verificar y transformar el sistema de coordenadas de los códigos postales
if codigo_postal_gdf.crs != "EPSG:4326":
    codigo_postal_gdf = codigo_postal_gdf.to_crs("EPSG:4326")

# Convertir el DataFrame de Spark a Pandas para usar GeoPandas
df_pandas = df.toPandas()

# Transformar las coordenadas de lat/lon a EPSG:4326 si es necesario
transformer = Transformer.from_crs("EPSG:4326", "EPSG:4326", always_xy=True)

# Función para encontrar la comuna a partir de latitud y longitud
def find_comuna(lat, lon):
    point = Point(lon, lat)
    for _, row in neighborhoods_gdf.iterrows():
        if row['geometry'].contains(point):
            return row['IDENTIFICACION']
    return None

# Función para encontrar el código postal a partir de latitud y longitud
def find_codigo_postal(lat, lon):
    x, y = transformer.transform(lon, lat)
    point = Point(x, y)
    for _, row in codigo_postal_gdf.iterrows():
        if row['geometry'].contains(point):
            return str(row['codigo_postal'])
    return None

# Aplicar la función find_comuna a cada fila
df_pandas['comuna'] = df_pandas.apply(lambda row: find_comuna(row['latitude'], row['longitude']), axis=1)

# Aplicar la función find_codigo_postal a cada fila
df_pandas['codigo_postal'] = df_pandas.apply(lambda row: find_codigo_postal(row['latitude'], row['longitude']), axis=1)

# Definir el esquema final del DataFrame
schema_final = StructType([
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("quantity_products", IntegerType(), True),
    StructField("order_id", StringType(), True),
    StructField("day", StringType(), True),
    StructField("month", StringType(), True),
    StructField("year", StringType(), True),
    StructField("time", StringType(), True),
    StructField("comuna", StringType(), True),
    StructField("codigo_postal", StringType(), True)
])

# Convertir de nuevo a Spark DataFrame con el esquema definido
df_spark = spark.createDataFrame(df_pandas, schema=schema_final)

# Escribir el DataFrame resultante a HDFS en formato Parquet con compresión Gzip usando append
df_spark.write.mode('append').parquet("hdfs://172.17.0.2:9000/silver_data", compression='gzip')

# Lectura y escritura del archivo customers.parquet
customers_df = spark.read.parquet("./base.data/customers.parquet")
customers_df.write.mode('append').parquet("hdfs://172.17.0.2:9000/customers", compression='gzip')

# Lectura y escritura del archivo employees.parquet
employees_df = spark.read.parquet("./base.data/employees.parquet")
employees_df.write.mode('append').parquet("hdfs://172.17.0.2:9000/employees", compression='gzip')

spark.stop()


