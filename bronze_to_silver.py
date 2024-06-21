from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StringType, StructType, StructField, FloatType, IntegerType
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from pyproj import Transformer

# Inicializar Spark
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .getOrCreate()

try:
    # Leer el archivo parquet desde HDFS
    df = spark.read.parquet("hdfs://172.17.0.2:9000/bronze_data")

    # Transformación 1: Separar la columna 'date' en día, mes, año y tiempo
    df = df.withColumn("day", split(col("date"), " ")[0].substr(9, 2).cast(StringType())) \
           .withColumn("month", split(col("date"), " ")[0].substr(6, 2).cast(StringType())) \
           .withColumn("year", split(col("date"), " ")[0].substr(0, 4).cast(StringType())) \
           .withColumn("time", split(col("date"), " ")[1].cast(StringType()))

    # Eliminar la columna 'date'
    df = df.drop("date")

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
        StructField("customer_id", IntegerType(), True),
        StructField("employee_id", IntegerType(), True),
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

    # Eliminar duplicados
    df_spark = df_spark.dropDuplicates(["order_id"])

    # Escribir el DataFrame resultante a HDFS en formato Parquet con compresión Gzip usando append
    df_spark.write.mode('append').parquet("hdfs://172.17.0.2:9000/silver_data", compression='gzip')

except Exception as e:
    print(f"Error: {e}")

finally:
    spark.stop()


