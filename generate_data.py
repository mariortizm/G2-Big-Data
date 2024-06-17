import geopandas as gpd
from shapely.geometry import Point
import random
import socket
import time
from datetime import datetime
import uuid
import json

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

interval_seconds = 2

def send_event_to_spark(host='localhost', port=50020):  
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()
        print(f"Listening for connections on {host}:{port}...")

        while True:
            try:
                client_socket, client_address = server_socket.accept()
                with client_socket:
                    print(f"Connection established with {client_address}")
                    while True:
                        event = generate_random_event()
                        event_json = json.dumps(event)
                        print("Sending event:", event)  # Línea para imprimir el evento que se está enviando
                        client_socket.sendall(event_json.encode('utf-8'))
                        time.sleep(interval_seconds)
            except BrokenPipeError:
                print("BrokenPipeError: Connection lost, waiting for new connection.")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    send_event_to_spark()