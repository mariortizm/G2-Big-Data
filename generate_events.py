import geopandas as gpd
import random
from shapely.geometry import Point
from datetime import datetime
import uuid
import time
import pandas as pd
from hdfs import InsecureClient
from pyhive import hive

# Conectar al cliente HDFS
client = InsecureClient('http://localhost:50070', user='root')

# Leer el archivo parquet y convertirlo en un GeoDataFrame
gdf = gpd.read_parquet('/workspace/base.data/medellin_neighborhoods.parquet')

# Crear una función para generar un punto aleatorio dentro de un polígono
def generate_random_point_in_polygon(polygon):
    minx, miny, maxx, maxy = polygon.bounds
    while True:
        pnt = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
        if polygon.contains(pnt):
            return pnt

# Modificar la función generate_random_event para generar coordenadas dentro de los límites de Medellín
def generate_random_event():
    polygon = random.choice(gdf['geometry'])
    pnt = generate_random_point_in_polygon(polygon)
    event = {
        "latitude": pnt.y,
        "longitude": pnt.x,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": random.randint(1000, 9999),
        "employee_id": random.randint(1000, 9999),
        "quantity_products": random.randint(1, 100),
        "order_id": str(uuid.uuid4())
    }
    return event

# Conectar al servidor Hive
conn = hive.Connection(host='localhost', port=10000, username='root')

def store_events_in_hdfs(events, path='/user/hadoop_user/events/'):
    # Convertir los eventos en un DataFrame
    df = pd.DataFrame(events)

    # Crear un nombre de archivo único basado en el timestamp actual
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{path}events_{timestamp}.parquet"

    # Escribir el DataFrame en un archivo Parquet temporal
    temp_file = f"/tmp/events_{timestamp}.parquet"
    df.to_parquet(temp_file, index=False)

    # Subir el archivo Parquet a HDFS
    client.upload(file_path, temp_file)

    # Crear la tabla en Hive si no existe y añadir la partición correspondiente
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS events (
            latitude DOUBLE,
            longitude DOUBLE,
            date STRING,
            customer_id INT,
            employee_id INT,
            quantity_products INT,
            order_id STRING
        ) PARTITIONED BY (year STRING, month STRING, day STRING)
        STORED AS PARQUET
        LOCATION '{path}'
    """)

    # Añadir la partición correspondiente
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")
    cursor.execute(f"""
        ALTER TABLE events ADD IF NOT EXISTS PARTITION (year='{year}', month='{month}', day='{day}')
    """)

# Loop para generar y almacenar eventos cada 30 segundos
events_batch = []
batch_size = 10  # Ajusta el tamaño del lote según sea necesario

while True:
    event = generate_random_event()
    events_batch.append(event)
    if len(events_batch) >= batch_size:
        store_events_in_hdfs(events_batch)
        events_batch = []
    time.sleep(30)
