import geopandas as gpd
from shapely.geometry import Point
import random
import socket
import time
from datetime import datetime, timedelta
import uuid
import json
import pandas as pd

# Cargar GeoDataFrame
gdf = gpd.read_parquet('./base.data/medellin_neighborhoods.parquet')

# Cargar employee_id y customer_id desde archivos parquet
employees_df = pd.read_parquet('./base.data/employees.parquet')
customers_df = pd.read_parquet('./base.data/customers.parquet')

employee_ids = employees_df['employee_id'].tolist()
customer_ids = customers_df['customer_id'].tolist()

def generate_random_point_in_polygon(polygon):
    minx, miny, maxx, maxy = polygon.bounds
    while True:
        pnt = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
        if polygon.contains(pnt):
            return pnt

def generate_random_date(start_date, end_date):
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    random_timestamp = random.randint(start_timestamp, end_timestamp)
    return datetime.fromtimestamp(random_timestamp)

def generate_random_event():
    polygon = random.choice(gdf['geometry'])
    pnt = generate_random_point_in_polygon(polygon)
    random_date = generate_random_date(datetime(2021, 1, 1, 0, 0, 0), datetime(2024, 6, 21, 0, 0, 0))
    return {
        "latitude": pnt.y,
        "longitude": pnt.x,
        "date": random_date.strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": random.choice(customer_ids),
        "employee_id": random.choice(employee_ids),
        "quantity_products": random.randint(1, 100),
        "order_id": str(uuid.uuid4())
    }

interval_seconds = 30

def send_event_to_spark(host='localhost', port=50014):  
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