import socket
import random
import uuid
from datetime import datetime
from shapely.geometry import Point
import geopandas as gpd
import time

# Cargar GeoDataFrame
gdf = gpd.read_parquet('./base.data/medellin_neighborhoods.parquet')

def generate_random_point_in_polygon(polygon):
    minx, miny, maxx, maxy = polygon.bounds
    while True:
        pnt = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
        if polygon.contains(pnt):
            return pnt

def generate_random_event():
    polygon = random.choice(gdf['geometry'])
    pnt = generate_random_point_in_polygon(polygon)
    return {
        "latitude": pnt.y,
        "longitude": pnt.x,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": random.randint(1000, 9999),
        "employee_id": random.randint(1000, 9999),
        "quantity_products": random.randint(1, 100),
        "order_id": str(uuid.uuid4())
    }

# Configurar el socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(("localhost", 9999))
server_socket.listen(1)
print("Esperando conexiones en el puerto 9999...")

connection, address = server_socket.accept()
print(f"Conexión establecida con {address}")

try:
    while True:
        event = generate_random_event()
        event_str = str(event)
        connection.sendall(event_str.encode('utf-8'))
        time.sleep(30)  # Espera 30 segundos antes de enviar el siguiente evento
finally:
    connection.close()
    server_socket.close()
