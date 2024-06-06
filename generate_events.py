import random
import uuid
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Point
import socket
import json
import time

# Conectar al servidor de socket
HOST = 'localhost'  # Dirección IP del servidor
PORT = 65432        # Puerto del servidor
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

print(f"Intentando conectar a {HOST}:{PORT}")
client_socket.connect((HOST, PORT))
print("Conexión establecida")

# Leer el archivo parquet y convertirlo en un GeoDataFrame
gdf = gpd.read_parquet('./base.data/medellin_neighborhoods.parquet')

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

# Variables de configuración
interval_seconds = 30
batch_duration_minutes = 5
batch_duration_seconds = batch_duration_minutes * 60
events_per_batch = batch_duration_seconds // interval_seconds

# Generar y enviar eventos al servidor de socket en intervalos regulares
while True:
    events = []
    start_time = datetime.now()
    for _ in range(events_per_batch):
        event = generate_random_event()
        events.append(event)
        time.sleep(interval_seconds)
    data = json.dumps(events)
    client_socket.sendall(data.encode())
    print(f"Batch sent: {len(events)} events")
