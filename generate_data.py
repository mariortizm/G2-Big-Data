import geopandas as gpd
from shapely.geometry import Point
import random
import socket
import time
from datetime import datetime
import uuid

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
    pnt = generate_random_wpoint_in_polygon(polygon)
    return {
        "latitude": pnt.y,
        "longitude": pnt.x,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": random.randint(1000, 9999),
        "employee_id": random.randint(1000, 9999),
        "quantity_products": random.randint(1, 100),
        "order_id": str(uuid.uuid4())
    }

interval_seconds = 30

def send_event_to_spark(host='localhost', port=5000):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    while True:
        event = generate_random_event()
        s.sendall(str(event).encode('utf-8'))
        time.sleep(interval_seconds)

if __name__ == '__main__':
    send_event_to_spark()